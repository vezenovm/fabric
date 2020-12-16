package random_bft

import (
	"sync"
	"time"

	"code.cloudfoundry.org/clock"
	"github.com/hyperledger/fabric/common/flogging"
)

type node struct {
	chainID string
	logger  *flogging.FabricLogger

	unreachableLock sync.RWMutex
	unreachable     map[uint64]struct{}

	rpc RPC

	chain *Chain

	tickInterval time.Duration
	clock        clock.Clock
}

// func (n *node) start() {
// 	// Extra start functionality just as communication confgis can be added here later
// 	go n.run()
// }

// func (n *node) run() {
// 	for {
// 		select {
// 		case <-n.chain.haltC:
// 			n.Stop()
// 			n.logger.Infof("node stopped")
// 			close(n.chain.doneC) // close after all the artifacts are closed
// 			return
// 		default:
// 			n.send
// 		}
// 	}
// }
