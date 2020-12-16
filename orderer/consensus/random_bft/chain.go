package random_bft

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"code.cloudfoundry.org/clock"
	"github.com/hyperledger/fabric-protos-go/common"
	cb "github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric-protos-go/orderer"
	"github.com/hyperledger/fabric/bccsp"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/orderer/common/cluster"
	"github.com/hyperledger/fabric/orderer/common/types"
	"github.com/hyperledger/fabric/orderer/consensus"
)

// Configurator is used to configure the communication layer
// when the chain starts.
type Configurator interface {
	Configure(channel string, newNodes []cluster.RemoteNode)
}

// RPC is used to mock the transport layer in tests.
type RPC interface {
	SendConsensus(dest uint64, msg *orderer.ConsensusRequest) error
	SendSubmit(dest uint64, request *orderer.SubmitRequest) error
}

type message struct {
	configSeq uint64
	normalMsg *cb.Envelope
	configMsg *cb.Envelope
}

// Chain implements consensus.Chain interface.
type Chain struct {
	configurator Configurator

	rpc RPC

	channelID string

	ActiveNodes atomic.Value

	// sendChan chan *message
	// exitChan chan struct{}

	submitC chan *orderer.SubmitRequest
	haltC   chan struct{} // Signals to goroutines that the chain is halting
	doneC   chan struct{} // Closes when the chain halts
	startC  chan struct{} // Closes when the node is started

	errorCLock sync.RWMutex
	errorC     chan struct{} // returned by Errored()

	clock clock.Clock // Tests can inject a fake clock

	support consensus.ConsenterSupport

	lastBlock    *common.Block
	appliedIndex uint64

	Node *node

	logger *flogging.FabricLogger

	haltCallback func()

	statusReportMutex sync.Mutex
	status            types.Status

	currentProposedValues	map[[]byte]int
	currNumberProposals		uint64

	currRatifiedValues		map[[]byte]int
	currRatifiedProposals	uint64

	coin					uint32

	// BCCSP instance
	CryptoProvider bccsp.BCCSP
}

func newChain(
	support consensus.ConsenterSupport,
	conf Configurator,
	rpc RPC,
	cryptoProvider bccsp.BCCSP,
	f CreateBlockPuller,
	haltCallback func()
) (*Chain, error) {

	c := &Chain{
		configurator:      conf,
		rpc:               rpc,
		channelID:         support.ChannelID(),
		submitC:           make(chan *orderer.SubmitRequest),
		haltC:             make(chan struct{}),
		doneC:             make(chan struct{}),
		startC:            make(chan struct{}),
		errorC:            make(chan struct{}),
		support:           support,
		appliedIndex:      opts.BlockMetadata.RaftIndex,
		lastBlock:         b,
		clock:             opts.Clock,
		haltCallback:      haltCallback,
		consensusRelation: types.ConsensusRelationConsenter,
		status:            types.StatusActive,
		logger:         lg,
		opts:           opts,
		CryptoProvider: cryptoProvider,
	}

	disseminator := &Disseminator{RPC: c.rpc}
	disseminator.UpdateMetadata(nil) // initialize
	c.ActiveNodes.Store([]uint64{})

	c.Node = &node{
		chainID:      c.channelID,
		chain:        c,
		logger:       c.logger,
		rpc:          disseminator,
		tickInterval: c.opts.TickInterval,
		clock:        c.clock,
	}

	return c, nil
}

// Start instructs the orderer to begin serving the chain and keep it current.
func (c *Chain) Start() {
	c.logger.Infof("Starting node")

	c.Node.start()

	close(c.startC)
	close(c.errorC)

	go c.run()
}

// Errored returns a channel that closes when the chain stops.
func (c *Chain) Errored() <-chan struct{} {
	c.errorCLock.RLock()
	defer c.errorCLock.RUnlock()
	return c.errorC
}

// Halt stops the chain.
func (c *Chain) Halt() {
	c.stop()
}

func (c *Chain) stop() bool {
	select {
	case <-c.startC:
	default:
		c.logger.Warn("Attempted to halt a chain that has not started")
		return false
	}

	select {
	case c.haltC <- struct{}{}:
	case <-c.doneC:
		return false
	}
	<-c.doneC
	return true
}

// halt stops the chain and calls the haltCallback function, which allows the
// chain to transfer responsibility to a follower or the inactive chain registry when a chain
// discovers it is no longer a member of a channel.
// Taken from Hyperledger boilerplate, might be very important for recovery modes in the future
func (c *Chain) halt() {
	if stopped := c.stop(); !stopped {
		c.logger.Info("This node was stopped, the haltCallback will not be called")
		return
	}
	if c.haltCallback != nil {
		c.haltCallback() // Must be invoked WITHOUT any internal lock
	}
}

func (c *Chain) isRunning() error {
	select {
	case <-c.startC:
	default:
		return errors.Errorf("chain is not started")
	}

	select {
	case <-c.doneC:
		return errors.Errorf("chain is stopped")
	default:
	}

	return nil
}

func (ch *Chain) WaitReady() error {
	return nil
}

// Order submits normal type transactions for ordering.
func (c *Chain) Order(env *common.Envelope, configSeq uint64) error {
	return c.Submit(&orderer.SubmitRequest{LastValidationSeq: configSeq, Payload: env, Channel: c.channelID}, 0)
}

// Configure submits config type transactions for ordering.
func (c *Chain) Configure(env *common.Envelope, configSeq uint64) error {
	return c.Submit(&orderer.SubmitRequest{LastValidationSeq: configSeq, Payload: env, Channel: c.channelID}, 0)
}

// Submit forwards the incoming request to:
// - every other node in the system to be voted on
// - Unimplemented this method needs to accurately send requests to all 
func (c *Chain) Submit(req *orderer.SubmitRequest, sender uint64) error {
	if err := c.isRunning(); err != nil {
		return err
	}
	select {
	case c.submitC <- req:
		// if err := c.rpc.SendSubmit(lead, req); err != nil {
		// 	return err
		// }
	case <-c.doneC:
		return errors.Errorf("chain is stopped")
	}

	return nil
}

func (c *Chain) run() {
	ticking := false
	timer := c.clock.NewTimer(time.Second)
	// we need a stopped timer rather than nil,
	// because we will be select waiting on timer.C()
	if !timer.Stop() {
		<-timer.C()
	}

	// if timer is already started, this is a no-op
	startTimer := func() {
		if !ticking {
			ticking = true
			timer.Reset(c.support.SharedConfig().BatchTimeout())
		}
	}
	
	stopTimer := func() {
		if !timer.Stop() && ticking {
			// we only need to drain the channel if the timer expired (not explicitly stopped)
			<-timer.C()
		}
		ticking = false
	}
	

	submitC := c.submitC
	var bc *blockCreator

	// Block to be proposed
	var propC chan<- *common.Block
	var ratifyCh chan<- []byte
	var cancelProp context.CancelFunc
	cancelProp = func() {} // no-op as initial value

	for {
		select {
		case s := <-submitC:
			if s == nil {
				continue
			}

			batches, pending, err := c.ordered(s.req)
			if err != nil {
				c.logger.Errorf("Failed to order message: %s", err)
				continue
			}
			if pending {
				startTimer() // no-op if timer is already started
			} else {
				stopTimer()
			}

			// Initialization of block creator for block proposal
			bc = &blockCreator{
				hash:   protoutil.BlockHeaderHash(c.lastBlock.Header),
				number: c.lastBlock.Header.Number,
				logger: c.logger,
			}

			c.propose(propC, bc, batches...)

			ctx, cancel := context.WithCancel(context.Background())
			go c.first_round(ctx, propC, ratifyCh)
			go c.ratify(ctx, propC, ratifyCh)


		case <-timer.C():
			ticking = false

			batch := c.support.BlockCutter().Cut()
			if len(batch) == 0 {
				c.logger.Warningf("Batch timer expired with no pending requests, this might indicate a bug")
				continue
			}

			c.logger.Debugf("Batch timer expired, creating block")
			c.propose(propC, bc, batch) // we are certain this is normal block, no need to block

		case <-c.doneC:
			stopTimer()
			cancelProp()

			select {
			case <-c.errorC: // avoid closing closed channel
			default:
				close(c.errorC)
			}

			c.logger.Infof("Stop serving requests")
			return
		}
	}
}

// Orders the envelope in the `msg` content. SubmitRequest.
// This method is the same as in Fabric's in-house etcdraft implementation
// as this method is simply responsible for creating the appropriate batches to put
// into blocks. This same sequence of statements can be found in the Solo
// as well in the main method for the chain.
// Returns
//   -- batches [][]*common.Envelope; the batches cut,
//   -- pending bool; if there are envelopes pending to be ordered,
//   -- err error; the error encountered, if any.
// It takes care of config messages as well as the revalidation of messages if the config sequence has advanced.
func (c *Chain) ordered(msg *orderer.SubmitRequest) (batches [][]*common.Envelope, pending bool, err error) {
	seq := c.support.Sequence()

	if c.isConfig(msg.Payload) {
		// ConfigMsg
		if msg.LastValidationSeq < seq {
			c.logger.Warnf("Config message was validated against %d, although current config seq has advanced (%d)", msg.LastValidationSeq, seq)
			msg.Payload, _, err = c.support.ProcessConfigMsg(msg.Payload)
			if err != nil {
				return nil, true, errors.Errorf("bad config message: %s", err)
			}
		}

		batch := c.support.BlockCutter().Cut()
		batches = [][]*common.Envelope{}
		if len(batch) != 0 {
			batches = append(batches, batch)
		}
		batches = append(batches, []*common.Envelope{msg.Payload})
		return batches, false, nil
	}
	// it is a normal message
	if msg.LastValidationSeq < seq {
		c.logger.Warnf("Normal message was validated against %d, although current config seq has advanced (%d)", msg.LastValidationSeq, seq)
		if _, err := c.support.ProcessNormalMsg(msg.Payload); err != nil {
			return nil, true, errors.Errorf("bad normal message: %s", err)
		}
	}
	batches, pending = c.support.BlockCutter().Ordered(msg.Payload)
	return batches, pending, nil

}

func (c *Chain) propose(ch chan<- *common.Block, bc *blockCreator, batches ...[]*common.Envelope) {
	for _, batch := range batches {
		b := bc.createNextBlock(batch)
		c.logger.Infof("Created block [%d], there are %d blocks in flight", b.Header.Number, c.blockInflight)

		select {
		case ch <- b:
		default:
			c.logger.Panic("Programming error: limit of in-flight blocks does not properly take effect or block is proposed by follower")
		}
	}
}


// Main consensus algorithm
// TODO: need to implement keeping track of the round associated with each message
// Should put the types of the values being passed into the channels into structs that also hold their message round
// and then the objects to be passed will be those structs
func (c *Chain) ratify(ctx context.Context, ch chan <- *common.Block, ratifyCh chan<- []byte) {
	for {
		select {

		// This is on the receiving end of the ratification channel from Chain.ratify
		case bytes := <-ratifyCh:

			
			// Updated the currently proposed values 
			if c.currRatifiedValues[bytes] == 0 {
				c.currRatifiedValues[bytes] = 1
			} else {
				currRatifiedValues[bytes]++
			}
			c.currRatifiedProposals++
			// First we must check that we have received at least n - t reponses
			// where t < n/2 possible crashes
			if c.currRatifiedProposals >= atomic.LoadInt64(c.ActiveNodes) / 2 {
				for value, numVals := range c.currentRatifiedValues {
					if numVals >= atomic.LoadInt64(c.ActiveNodes) / 2 {
						block := protoutil.UnmarshalBlockOrPanic(value)
						c.writeBlock(block)
						return
					}
				}
				block := protoutil.UnmarshalBlockOrPanic(bytes)
				ch := block
			}
			c.logger.Debugf("Proposed block [%d] to random bft consensus", b.Header.Number)

		case <-ctx.Done():
			c.logger.Debugf("Quit proposing blocks, discarded %d blocks in the queue", len(ch))
			return
		default:
			ratifyCh := c.coinFlip()
		}
	}
}

func (c *Chain) first_round(ctx context.Context, ch chan<- *common.Block, ratifyCh chan <- []byte) {
	for {
		select {

		// This is on the receiving end of the block creation channel from Chain.propose
		case b := <-ch:
			data := protoutil.MarshalOrPanic(b)
			
			// Updated the currently proposed values 
			if c.currentProposedValues[data] == 0 {
				c.currentProposedValues[data] = 1
			} else {
				currentProposedValues[data]++
			}
			c.currentNumberProposals++
			// First we must check that we have received at least n - t reponses
			// where t < n/2 possible crashes
			if c.currentNumberProposals >= atomic.LoadInt64(c.ActiveNodes) / 2 {
				for value, numVals := range c.currentProposedValues {
					if numVals > atomic.LoadInt64(c.ActiveNodes) / 2 {
						ratifyCh := value
					} else {
						ratifyCh := nil
					}
				}
			}
			c.logger.Debugf("Proposed block [%d] to random bft consensus", b.Header.Number)

		case <-ctx.Done():
			c.logger.Debugf("Quit proposing blocks, discarded %d blocks in the queue", len(ch))
			return
		}
	}
}

func (c *Chain) writeBlock(block *common.Block, index uint64) {
	if block.Header.Number > c.lastBlock.Header.Number+1 {
		c.logger.Panicf("Got block [%d], expect block [%d]", block.Header.Number, c.lastBlock.Header.Number+1)
	} else if block.Header.Number < c.lastBlock.Header.Number+1 {
		// TODO: implement the chain catching up to the peers it has fallen behind
		c.logger.Infof("Got block [%d], expect block [%d], this node was forced to catch up", block.Header.Number, c.lastBlock.Header.Number+1)
		return
	}

	c.lastBlock = block

	if protoutil.IsConfigBlock(block) {
		c.writeConfigBlock(block, index)
		return
	}
	m := protoutil.MarshalOrPanic(c.opts.BlockMetadata)
	c.support.WriteBlock(block, m)
}

func (c *Chain) coinFlip() {
	if coin == 1 {
		coin--
	} else {
		coin++
	}
	return c.coin
}
