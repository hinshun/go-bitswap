package notifications

import (
	"context"
	"sync"
	"time"

	pubsub "github.com/cskr/pubsub"
	"github.com/google/uuid"
	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log"
	opentracing "github.com/opentracing/opentracing-go"
)

var log = logging.Logger("bitswap")

const bufferSize = 16

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
		ctx:         ctx,
		wrapped:     pubsub.New(bufferSize),
		subscribers: make(map[cid.Cid]map[string]chan<- blocks.Block),
		refcount:    make(map[string]int),
		closed:      make(chan struct{}),
	}

	go func() {
		if opentracing.SpanFromContext(ctx) == nil {
			return
		}

		for _ = range time.Tick(time.Second) {
			log.LogKV(ctx,
				"event", "pubSubTick",
				"numPublish", ps.numPublish,
				"numSubscribe", ps.numSubscribe,
				"culmTimeWaitingToPublish", ps.culmTimeWaitingToPublish,
				"culmTimeWaitingToSubscribe", ps.culmTimeWaitingToSubscribe,
			)
		}
	}()

	return ps
}

type impl struct {
	lk      sync.RWMutex
	wrapped *pubsub.PubSub

	closed chan struct{}

	mu          sync.RWMutex
	subscribers map[cid.Cid]map[string]chan<- blocks.Block
	refcount    map[string]int

	ctx                        context.Context
	numPublish                 int
	numSubscribe               int
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

	ps.mu.Lock()
	for id, subscriber := range ps.subscribers[block.Cid()] {
		subscriber <- block
		delete(ps.subscribers[block.Cid()], id)
		ps.refcount[id]--

		if ps.refcount[id] == 0 {
			close(subscriber)
			delete(ps.refcount, id)
		}
	}
	ps.mu.Unlock()

	ps.wrapped.Pub(block, block.Cid().KeyString())
	ps.culmTimeWaitingToPublish += time.Now().Sub(start)
	ps.numPublish++
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

	id := uuid.New().String()

	ps.mu.Lock()
	ps.refcount[id] = len(keys)
	for _, key := range keys {
		subscribers, ok := ps.subscribers[key]
		if !ok {
			subscribers = make(map[string]chan<- blocks.Block)
		}
		subscribers[id] = blocksCh
		ps.subscribers[key] = subscribers
	}
	ps.mu.Unlock()

	go func() {
		select {
		case <-ctx.Done():
		case <-ps.closed:
		}

		ps.mu.Lock()
		if ps.refcount[id] > 0 {
			close(blocksCh)
			delete(ps.refcount, id)
		}
		for _, key := range keys {
			delete(ps.subscribers[key], id)
		}
		ps.mu.Unlock()
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
