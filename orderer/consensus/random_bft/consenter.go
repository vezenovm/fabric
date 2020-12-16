/*
Copied version of Solo to see if I can correctly plug a service
*/

package random_bft

import (
	"reflect"

	"github.com/hyperledger/fabric-protos-go/common"
	cb "github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric/bccsp"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/orderer/common/cluster"
	"github.com/hyperledger/fabric/orderer/common/localconfig"
	"github.com/hyperledger/fabric/orderer/common/types"
	"github.com/hyperledger/fabric/orderer/consensus"
)

var logger = flogging.MustGetLogger("orderer.consensus.solo")

// InactiveChainRegistry registers chains that are inactive
type InactiveChainRegistry interface {
	// TrackChain tracks a chain with the given name, and calls the given callback
	// when this chain should be created.
	TrackChain(chainName string, genesisBlock *common.Block, createChain func())
	// Stop stops the InactiveChainRegistry. This is used when removing the
	// system channel.
	Stop()
}

// ChainManager defines the methods from multichannel.Registrar needed by the Consenter.
type ChainManager interface {
	GetConsensusChain(channelID string) consensus.Chain
	CreateChain(channelID string)
	SwitchChainToFollower(channelID string)
	ReportConsensusRelationAndStatusMetrics(channelID string, relation types.ConsensusRelation, status types.Status)
}

type Consenter struct {
	ChainManager          ChainManager
	InactiveChainRegistry InactiveChainRegistry

	OrdererConfig localconfig.TopLevel
	Communication cluster.Communicator
	*Dispatcher

	Cert   []byte
	BCCSP  bccsp.BCCSP
	Logger *flogging.FabricLogger
}

// ReceiverByChain returns the MessageReceiver for the given channelID or nil
// if not found.
func (c *Consenter) ReceiverByChain(channelID string) MessageReceiver {
	chain := c.ChainManager.GetConsensusChain(channelID)
	if chain == nil {
		return nil
	}
	if randomBftChain, isRandomBftChain := chain.(*Chain); isRandomBftChain {
		return randomBftChain
	}
	c.Logger.Warningf("Chain %s is of type %v and not etcdraft.Chain", channelID, reflect.TypeOf(chain))
	return nil
}

// New creates a new consenter for the consensus scheme
// It represents a wrapper around the entire chain and a node's
// communication with other ordering nodes
func New() consensus.Consenter {
	return &Consenter{}
}

func (solo *Consenter) HandleChain(support consensus.ConsenterSupport, metadata *cb.Metadata) (consensus.Chain, error) {
	return newChain(support), nil
}
