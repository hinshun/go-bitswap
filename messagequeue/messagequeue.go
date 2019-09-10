package messagequeue

import (
	"context"
	"fmt"
	"sync"
	"time"

	bsmsg "github.com/ipfs/go-bitswap/message"
	bsnet "github.com/ipfs/go-bitswap/network"
	"github.com/ipfs/go-bitswap/timer"
	wantlist "github.com/ipfs/go-bitswap/wantlist"
	logging "github.com/ipfs/go-log"
	peer "github.com/libp2p/go-libp2p-core/peer"
	opentracing "github.com/opentracing/opentracing-go"
)

var log = logging.Logger("bitswap")

const (
	defaultRebroadcastInterval = 30 * time.Second
	maxRetries                 = 10
)

// MessageNetwork is any network that can connect peers and generate a message
// sender.
type MessageNetwork interface {
	ConnectTo(context.Context, peer.ID) error
	NewMessageSender(context.Context, peer.ID) (bsnet.MessageSender, error)
}

// MessageQueue implements queue of want messages to send to peers.
type MessageQueue struct {
	ctx     context.Context
	p       peer.ID
	network MessageNetwork

	outgoingWork chan struct{}
	done         chan struct{}

	// for capturing metrics
	firstMessageSent bool

	// do not touch out of run loop
	wl                    *wantlist.SessionTrackedWantlist
	nextMessage           bsmsg.BitSwapMessage
	nextMessageLk         sync.RWMutex
	sender                bsnet.MessageSender
	rebroadcastIntervalLk sync.RWMutex
	rebroadcastInterval   time.Duration
	rebroadcastTimer      *time.Timer

	culmTimeWaitedToAddMessage time.Duration
}

// New creats a new MessageQueue.
func New(ctx context.Context, p peer.ID, network MessageNetwork) *MessageQueue {
	mq := &MessageQueue{
		ctx:                 ctx,
		wl:                  wantlist.NewSessionTrackedWantlist(),
		network:             network,
		p:                   p,
		outgoingWork:        make(chan struct{}, 1),
		done:                make(chan struct{}),
		rebroadcastInterval: defaultRebroadcastInterval,
	}

	go func() {
		if opentracing.SpanFromContext(ctx) == nil {
			return
		}

		for _ = range time.Tick(time.Second) {
			log.LogKV(ctx,
				"event", "messageQueueTick",
				"peer", p.Pretty(),
				"culmTimeWaitedToAddMessage", mq.culmTimeWaitedToAddMessage,
			)
		}
	}()

	return mq
}

// AddMessage adds new entries to an outgoing message for a given session.
func (mq *MessageQueue) AddMessage(entries []bsmsg.Entry, ses uint64) {
	if !mq.addEntries(entries, ses) {
		return
	}

	start := time.Now()
	select {
	case mq.outgoingWork <- struct{}{}:
		mq.culmTimeWaitedToAddMessage += time.Now().Sub(start)
	default:
	}
}

// AddWantlist adds a complete session tracked want list to a message queue
func (mq *MessageQueue) AddWantlist(initialWants *wantlist.SessionTrackedWantlist) {
	initialWants.CopyWants(mq.wl)
	mq.addWantlist()
}

// SetRebroadcastInterval sets a new interval on which to rebroadcast the full wantlist
func (mq *MessageQueue) SetRebroadcastInterval(delay time.Duration) {
	mq.rebroadcastIntervalLk.Lock()
	mq.rebroadcastInterval = delay
	if mq.rebroadcastTimer != nil {
		mq.rebroadcastTimer.Reset(delay)
	}
	mq.rebroadcastIntervalLk.Unlock()
}

// Startup starts the processing of messages, and creates an initial message
// based on the given initial wantlist.
func (mq *MessageQueue) Startup() {
	mq.rebroadcastIntervalLk.RLock()
	mq.rebroadcastTimer = time.NewTimer(mq.rebroadcastInterval)
	mq.rebroadcastIntervalLk.RUnlock()
	go mq.runQueue()
}

// Shutdown stops the processing of messages for a message queue.
func (mq *MessageQueue) Shutdown() {
	close(mq.done)
}

func (mq *MessageQueue) runQueue() {
	for {
		select {
		case <-mq.rebroadcastTimer.C:
			mq.rebroadcastWantlist()
		case <-mq.outgoingWork:
			mq.sendMessage()
		case <-mq.done:
			if mq.sender != nil {
				mq.sender.Close()
			}
			return
		case <-mq.ctx.Done():
			if mq.sender != nil {
				_ = mq.sender.Reset()
			}
			return
		}
	}
}

func (mq *MessageQueue) addWantlist() {

	mq.nextMessageLk.Lock()
	defer mq.nextMessageLk.Unlock()

	if mq.wl.Len() > 0 {
		if mq.nextMessage == nil {
			mq.nextMessage = bsmsg.New(false)
		}
		for _, e := range mq.wl.Entries() {
			mq.nextMessage.AddEntry(e.Cid, e.Priority, e.WantType, e.SendDontHave)
		}
		select {
		case mq.outgoingWork <- struct{}{}:
		default:
		}
	}
}

func (mq *MessageQueue) rebroadcastWantlist() {
	mq.rebroadcastIntervalLk.RLock()
	mq.rebroadcastTimer.Reset(mq.rebroadcastInterval)
	mq.rebroadcastIntervalLk.RUnlock()

	mq.addWantlist()
}

func (mq *MessageQueue) addEntries(entries []bsmsg.Entry, ses uint64) bool {
	var work bool
	mq.nextMessageLk.Lock()
	defer mq.nextMessageLk.Unlock()
	// if we have no message held allocate a new one
	if mq.nextMessage == nil {
		mq.nextMessage = bsmsg.New(false)
	}

	for _, e := range entries {
		if e.Cancel {
			if mq.wl.Remove(e.Cid, ses, e.WantType) {
				work = true
				mq.nextMessage.Cancel(e.Cid)
			}
		} else {
			if mq.wl.Add(e.Cid, e.Priority, e.WantType, e.SendDontHave, ses) {
				work = true
				mq.nextMessage.AddEntry(e.Cid, e.Priority, e.WantType, e.SendDontHave)
			}
		}
	}
	return work
}

func (mq *MessageQueue) extractOutgoingMessage() bsmsg.BitSwapMessage {
	// grab outgoing message
	mq.nextMessageLk.Lock()
	message := mq.nextMessage
	mq.nextMessage = nil
	mq.nextMessageLk.Unlock()
	return message
}

func (mq *MessageQueue) sendMessage() {
	message := mq.extractOutgoingMessage()
	if message == nil || message.Empty() {
		return
	}

	entries := message.Wantlist()
	wblocks := 0
	whaves := 0
	cwblocks := 0
	cwhaves := 0
	for _, e := range entries {
		if e.WantType == wantlist.WantType_Have {
			if e.Cancel {
				cwhaves++
			} else {
				whaves++
			}
		} else {
			if e.Cancel {
				cwblocks++
			} else {
				wblocks++
			}
		}
	}
	detail := ""
	if wblocks > 0 {
		detail += fmt.Sprintf(" %d want-block", wblocks)
	}
	if cwblocks > 0 {
		detail += fmt.Sprintf(" %d cancel-block", cwblocks)
	}
	if whaves > 0 {
		detail += fmt.Sprintf(" %d want-have", whaves)
	}
	if cwhaves > 0 {
		detail += fmt.Sprintf(" %d cancel-have", cwhaves)
	}
	log.Infof("Sending message to %s with %d entries:%s\n", mq.p, len(entries), detail)
	// for _, e := range entries {
	// 	log.Debugf("  %s: Cancel? %t / WantHave %t / SendDontHave %t\n", e.Cid.String()[2:8], e.Cancel, e.WantHave, e.SendDontHave)
	// }

	err := mq.initializeSender()
	if err != nil {
		log.Infof("cant open message sender to peer %s: %s", mq.p, err)
		// TODO: cant connect, what now?
		return
	}

	for i := 0; i < maxRetries; i++ { // try to send this message until we fail.
		if mq.attemptSendAndRecovery(message) {
			if opentracing.SpanFromContext(mq.ctx) != nil && !mq.firstMessageSent {
				mq.firstMessageSent = true
				log.LogKV(mq.ctx,
					"event", "firstMessageSent",
					"time", time.Now().Sub(timer.Time(mq.ctx)),
				)
			}
			return
		}
	}
}

func (mq *MessageQueue) initializeSender() error {
	if mq.sender != nil {
		return nil
	}
	nsender, err := openSender(mq.ctx, mq.network, mq.p)
	if err != nil {
		return err
	}
	mq.sender = nsender
	return nil
}

func (mq *MessageQueue) attemptSendAndRecovery(message bsmsg.BitSwapMessage) bool {
	err := mq.sender.SendMsg(mq.ctx, message)
	if err == nil {
		return true
	}

	log.Infof("bitswap send error: %s", err)
	_ = mq.sender.Reset()
	mq.sender = nil

	select {
	case <-mq.done:
		return true
	case <-mq.ctx.Done():
		return true
	case <-time.After(time.Millisecond * 100):
		// wait 100ms in case disconnect notifications are still propogating
		log.Warning("SendMsg errored but neither 'done' nor context.Done() were set")
	}

	err = mq.initializeSender()
	if err != nil {
		log.Infof("couldnt open sender again after SendMsg(%s) failed: %s", mq.p, err)
		// TODO(why): what do we do now?
		// I think the *right* answer is to probably put the message we're
		// trying to send back, and then return to waiting for new work or
		// a disconnect.
		return true
	}

	// TODO: Is this the same instance for the remote peer?
	// If its not, we should resend our entire wantlist to them
	/*
		if mq.sender.InstanceID() != mq.lastSeenInstanceID {
			wlm = mq.getFullWantlistMessage()
		}
	*/
	return false
}

func openSender(ctx context.Context, network MessageNetwork, p peer.ID) (bsnet.MessageSender, error) {
	// allow ten minutes for connections this includes looking them up in the
	// dht dialing them, and handshaking
	conctx, cancel := context.WithTimeout(ctx, time.Minute*10)
	defer cancel()

	err := network.ConnectTo(conctx, p)
	if err != nil {
		return nil, err
	}

	nsender, err := network.NewMessageSender(ctx, p)
	if err != nil {
		return nil, err
	}

	return nsender, nil
}
