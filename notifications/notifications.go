package notifications

import (
	"context"
	"sync"
	"time"

	pubsub "github.com/cskr/pubsub"
	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log"
	opentracing "github.com/opentracing/opentracing-go"
)

var log = logging.Logger("bitswap")

const bufferSize = 64

// PubSub is a simple interface for publishing blocks and being able to subscribe
// for cids. It's used internally by bitswap to decouple receiving blocks
// and actually providing them back to the GetBlocks caller.
type PubSub interface {
	Publish(block blocks.Block)
	Subscribe(ctx context.Context, keys ...cid.Cid) <-chan blocks.Block
	Shutdown()
}

// New generates a new PubSub interface.
func New(ctx context.Context) PubSub {
	ps := &impl{
		ctx:     ctx,
		wrapped: *pubsub.New(bufferSize),
		closed:  make(chan struct{}),
	}

	go func() {
		if opentracing.SpanFromContext(ctx) == nil {
			return
		}

		for _ = range time.Tick(time.Second) {
			log.LogKV(ctx,
				"event", "pubSubTick",
				"culmTimeWaitingToPublish", ps.culmTimeWaitingToPublish,
				"culmTimeWaitingToSubscribe", ps.culmTimeWaitingToSubscribe,
			)
		}
	}()

	return ps
}

type impl struct {
	lk      sync.RWMutex
	wrapped pubsub.PubSub

	closed chan struct{}

	ctx                        context.Context
	culmTimeWaitingToPublish   time.Duration
	culmTimeWaitingToSubscribe time.Duration
}

func (ps *impl) Publish(block blocks.Block) {
	ps.lk.RLock()
	defer ps.lk.RUnlock()
	select {
	case <-ps.closed:
		return
	default:
	}

	start := time.Now()
	ps.wrapped.Pub(block, block.Cid().KeyString())
	ps.culmTimeWaitingToPublish += time.Now().Sub(start)
}

func (ps *impl) Shutdown() {
	ps.lk.Lock()
	defer ps.lk.Unlock()
	select {
	case <-ps.closed:
		return
	default:
	}
	close(ps.closed)
	ps.wrapped.Shutdown()
}

// Subscribe returns a channel of blocks for the given |keys|. |blockChannel|
// is closed if the |ctx| times out or is cancelled, or after receiving the blocks
// corresponding to |keys|.
func (ps *impl) Subscribe(ctx context.Context, keys ...cid.Cid) <-chan blocks.Block {

	blocksCh := make(chan blocks.Block, len(keys))
	valuesCh := make(chan interface{}, len(keys)) // provide our own channel to control buffer, prevent blocking
	if len(keys) == 0 {
		close(blocksCh)
		return blocksCh
	}

	// prevent shutdown
	ps.lk.RLock()
	defer ps.lk.RUnlock()

	select {
	case <-ps.closed:
		close(blocksCh)
		return blocksCh
	default:
	}

	// AddSubOnceEach listens for each key in the list, and closes the channel
	// once all keys have been received
	start := time.Now()
	ps.wrapped.AddSubOnceEach(valuesCh, toStrings(keys)...)
	ps.culmTimeWaitingToSubscribe += time.Now().Sub(start)

	go func() {
		defer func() {
			close(blocksCh)

			ps.lk.RLock()
			defer ps.lk.RUnlock()
			// Don't touch the pubsub instance if we're
			// already closed.
			select {
			case <-ps.closed:
				return
			default:
			}

			ps.wrapped.Unsub(valuesCh)
		}()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ps.closed:
			case val, ok := <-valuesCh:
				if !ok {
					return
				}
				block, ok := val.(blocks.Block)
				if !ok {
					return
				}
				select {
				case <-ctx.Done():
					return
				case blocksCh <- block: // continue
				case <-ps.closed:
				}
			}
		}
	}()

	return blocksCh
}

func toStrings(keys []cid.Cid) []string {
	strs := make([]string, 0, len(keys))
	for _, key := range keys {
		strs = append(strs, key.KeyString())
	}
	return strs
}
