package chord

import (
	"math/big"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
)

const (
	stablizeInterval = 100 * time.Millisecond
)

func init() {
	f, _ := os.Create("dht-test.log")
	logrus.SetOutput(f)
}

type SingleNode struct {
	Addr string
	ID   *big.Int
}

type Node struct {
	SingleNode
	online          atomic.Bool
	server          NodeServer
	data            map[string]string
	dataLock        sync.RWMutex
	backupData      map[string]string
	backupDataLock  sync.RWMutex
	successor       SingleNode
	successorLock   sync.RWMutex
	predecessor     SingleNode
	predecessorLock sync.RWMutex
	fingerTable     [M]SingleNode
	fingerTableLock sync.RWMutex
}

type Pair struct {
	Key   string
	Value string
}

func (node *Node) Init(addr string) {
	node.Addr = addr
	node.ID = Hash(addr)
	node.data = make(map[string]string)
	node.backupData = make(map[string]string)
	node.server.Init(node)
	node.online.Store(false)
}

// followings are methods for interface dhtNode

/*Init method for interface dhtNode*/
func (node *Node) Run() {
	err := node.server.TurnOn("tcp", node.Addr)
	if err != nil {
		logrus.Errorf("Error <func Run()> turn on server [%s] error: %v\n", node.Addr, err)
	}
	node.online.Store(true)
}

func (node *Node) Create() {
	node.successor = SingleNode{node.Addr, node.ID}
	node.predecessor = SingleNode{node.Addr, node.ID}
	for i := 0; i < M; i++ {
		node.fingerTable[i] = SingleNode{node.Addr, node.ID}
	}
	node.maintainChord()
}

// following are helper functions for interface methods and RPC methods

func (node *Node) stablilize() {
	// TO DO
}

func (node *Node) maintainChord() {
	go func() {
		for node.online.Load() {
			node.stablilize()
			time.Sleep(stablizeInterval)
		}
	}()
}
