package chord

import (
	"fmt"
	"math/big"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
)

const (
	stablizeInterval   = 100 * time.Millisecond
	fixFingersInterval = 100 * time.Millisecond
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
	finger          [M]SingleNode
	fingerLock      sync.RWMutex
	fingerFixIndex  uint
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

/*Init method Run() for interface dhtNode*/
func (node *Node) Run() {
	err := node.server.TurnOn("tcp", node.Addr)
	if err != nil {
		logrus.Errorf("Error <func Run()> turn on node [%s] error: %v\n", node.getPort(), err)
	}
	node.online.Store(true)
}

/*Init method Create() for interface dhtNode*/
func (node *Node) Create() {
	logrus.Infof("Info <func Create()> node [%s] create a chord", node.getPort())
	node.successor = SingleNode{node.Addr, node.ID}
	node.predecessor = SingleNode{node.Addr, node.ID}
	var i uint
	for i = 0; i < M; i++ {
		node.finger[i] = SingleNode{node.Addr, node.ID}
	}
	node.maintainChord()
}

/*Init method Join() for interface dhtNode*/
func (node *Node) Join(addr string) bool {
	logrus.Infof("Info <func Join()> node [%s] join node [%s]", node.getPort(), getPortFromIP(addr))
	var suc SingleNode
	err := node.RemoteCall("tcp", addr, "RPC_Node.Find_successor", node.ID, &suc)
	if err != nil {
		logrus.Errorf("Error <func Join()> node [%s] call [%s] method [RPC_Node.Find_successor] error: %v", node.getPort(), getPortFromIP(addr), err)
		return false
	}
	node.set_successor(&suc)
	node.set_finger_i(0, &suc)
	node.maintainChord()
	return true
}

/*Init method Quit() for interface dhtNode*/
func (node *Node) Quit() {
}

/*Init method ForceQuit() for interface dhtNode*/
func (node *Node) ForceQuit() {
}

/*Init method Put() for interface dhtNode*/
func (node *Node) Put(key string, value string) bool {
	id := Hash(key)
	var suc SingleNode
	node.find_successor(id, &suc)
	err := node.RemoteCall("tcp", suc.Addr, "RPC_Node.PutData", Pair{key, value}, &struct{}{})
	if err != nil {
		logrus.Errorf("Error <func Put()> node [%s] call [%s] method [RPC_Node.PutData] error: %v", node.getPort(), suc.getPort(), err)
		return false
	}
	return true
}

/*Init method Get() for interface dhtNode*/
func (node *Node) Get(key string) (bool, string) {
	id := Hash(key)
	var suc SingleNode
	node.find_successor(id, &suc)
	var value string
	err := node.RemoteCall("tcp", suc.Addr, "RPC_Node.GetData", key, &value)
	if err != nil {
		logrus.Errorf("Error <func Get()> node [%s] call [%s] method [RPC_Node.GetData] error: %v", node.getPort(), suc.getPort(), err)
		return false, ""
	}
	return true, value
}

/*Init method Delete() for interface dhtNode*/
func (node *Node) Delete(key string) bool {
	id := Hash(key)
	var suc SingleNode
	node.find_successor(id, &suc)
	err := node.RemoteCall("tcp", suc.Addr, "RPC_Node.DeleteData", key, &struct{}{})
	if err != nil {
		logrus.Errorf("Error <func Delete()> node [%s] call [%s] method [RPC_Node.DeleteData] error: %v", node.getPort(), suc.getPort(), err)
		return false
	}
	return true
}

// following are helper functions for interface methods and RPC methods

// if id is in (begin, end) circularly then return true, o.w. return false
func in_range(id *big.Int, begin *big.Int, end *big.Int) bool {
	return end.Cmp(id)+id.Cmp(begin)+begin.Cmp(end) == 1 || (begin.Cmp(end) == 0 && begin.Cmp(id) != 0)
}

// return (id + (1 << i)) % (1 << M)
func calcID(id *big.Int, i uint) *big.Int {
	offset := new(big.Int).Lsh(big.NewInt(1), i)
	mod := new(big.Int).Lsh(big.NewInt(1), M)
	return new(big.Int).Mod(new(big.Int).Add(id, offset), mod)
}

func (node *Node) get_successor(res *SingleNode) error {
	node.successorLock.RLock()
	*res = node.successor
	node.successorLock.RUnlock()
	return nil
}

func (node *Node) get_predecessor(res *SingleNode) error {
	node.predecessorLock.RLock()
	*res = node.predecessor
	node.predecessorLock.RUnlock()
	return nil
}

func (node *Node) get_finger_i(i uint, res *SingleNode) error {
	node.fingerLock.RLock()
	*res = node.finger[i]
	node.fingerLock.RUnlock()
	return nil
}

func (node *Node) set_successor(n *SingleNode) error {
	node.successorLock.Lock()
	node.successor = *n
	node.successorLock.Unlock()
	return nil
}

func (node *Node) set_predecessor(n *SingleNode) error {
	node.predecessorLock.Lock()
	node.predecessor = *n
	node.predecessorLock.Unlock()
	return nil
}

func (node *Node) set_finger_i(i uint, n *SingleNode) error {
	node.fingerLock.Lock()
	node.finger[i] = *n
	node.fingerLock.Unlock()
	return nil
}

// find the successor of id
func (node *Node) find_successor(id *big.Int, res *SingleNode) error {
	if !node.online.Load() {
		logrus.Infof("Info <func find_successor()> node [%s] offline", node.getPort())
	}
	var suc SingleNode
	node.get_successor(&suc)
	if in_range(id, node.ID, suc.ID) || (id.Cmp(suc.ID) == 0) {
		*res = suc
		return nil
	}
	prec_finger := node.closest_preceding_finger(id)
	return node.RemoteCall("tcp", prec_finger.Addr, "RPC_Node.Find_successor", id, res)
}

func (node *Node) closest_preceding_finger(id *big.Int) SingleNode {
	if !node.online.Load() {
		logrus.Infof("Info <func closest_preceding_finger()> node [%s] offline", node.getPort())
	}
	var i uint
	for i = M - 1; i > 0; i-- {
		var f SingleNode
		node.get_finger_i(i, &f)
		if f.Addr == "" {
			continue
		}
		if in_range(f.ID, node.ID, id) {
			return f
		}
	}
	var suc SingleNode
	node.get_successor(&suc)
	return suc
}

func (node *Node) stablilize() error {
	logrus.Infof("Info <func stablilize()> node [%s] stablilize", node.getPort())
	// node.PrintNodeInfo()
	var suc, nSuc SingleNode
	node.get_successor(&suc)
	err := node.RemoteCall("tcp", suc.Addr, "RPC_Node.Get_predecessor", struct{}{}, &nSuc)
	if err != nil {
		logrus.Errorf("Error <func stablilize()> node [%s] call [%s] method [RPC_Node.Get_predecessor] error: %v", node.getPort(), suc.getPort(), err)
		return err
	}
	if in_range(nSuc.ID, node.ID, suc.ID) {
		node.set_successor(&nSuc)
		// node.set_finger_i(0, &nSuc)
	}
	node.get_successor(&suc)
	err = node.RemoteCall("tcp", suc.Addr, "RPC_Node.Notify", SingleNode{node.Addr, node.ID}, &struct{}{})
	if err != nil {
		logrus.Errorf("Error <func stablilize()> node [%s] call [%s] method [RPC_Node.Notify] error: %v", node.getPort(), suc.getPort(), err)
		return err
	}
	return nil
}

func (node *Node) notify(n SingleNode) error {
	var pre SingleNode
	node.get_predecessor(&pre)
	if pre.Addr == "" || in_range(n.ID, pre.ID, node.ID) {
		node.set_predecessor(&n)
	}
	return nil
}

func (node *Node) fix_fingers() {
	logrus.Infof("Info <func fix_fingers()> fix node [%s]'s fingers", node.getPort())
	var f, nf SingleNode
	node.find_successor(calcID(node.ID, node.fingerFixIndex), &nf)
	node.get_finger_i(node.fingerFixIndex, &f)
	if nf.Addr != f.Addr {
		node.set_finger_i(node.fingerFixIndex, &nf)
	}
	node.fingerFixIndex = (node.fingerFixIndex + 1) % M
}

func (node *Node) maintainChord() {
	go func() {
		for node.online.Load() {
			node.stablilize()
			time.Sleep(stablizeInterval)
		}
	}()
	go func() {
		for node.online.Load() {
			node.fix_fingers()
			time.Sleep(fixFingersInterval)
		}
	}()
}

func (node *Node) putData(pair Pair) error {
	node.dataLock.Lock()
	node.data[pair.Key] = pair.Value
	node.dataLock.Unlock()
	var suc SingleNode
	node.get_successor(&suc)
	err := node.RemoteCall("tcp", suc.Addr, "RPC_Node.PutBackupData", pair, &struct{}{})
	if err != nil {
		logrus.Errorf("Error <func putData()> node [%s] call [%s] method [RPC_Node.PutBackupData] error: %v", node.getPort(), suc.getPort(), err)
		return err
	}
	return nil
}

func (node *Node) putBackupData(pair Pair) error {
	node.backupDataLock.Lock()
	node.backupData[pair.Key] = pair.Value
	node.backupDataLock.Unlock()
	return nil
}

func (node *Node) getData(key string, value *string) error {
	node.dataLock.RLock()
	*value = node.data[key]
	node.dataLock.RUnlock()
	return nil
}

func (node *Node) deleteData(key string) error {
	node.dataLock.Lock()
	_, ok := node.data[key]
	delete(node.data, key)
	node.dataLock.Unlock()
	if !ok {
		return fmt.Errorf("no key [%s] on node [%s]'s data", key, node.getPort())
	}
	var suc SingleNode
	node.get_successor(&suc)
	err := node.RemoteCall("tcp", suc.Addr, "RPC_Node.DeleteBackupData", key, &struct{}{})
	if err != nil {
		logrus.Errorf("Error <func deleteData()> node [%s] call [%s] method [RPC_Node.DeleteBackupData] error: %v", node.getPort(), suc.getPort(), err)
		return err
	}
	return nil
}

func (node *Node) deleteBackupData(key string) error {
	node.backupDataLock.Lock()
	_, ok := node.backupData[key]
	delete(node.backupData, key)
	node.backupDataLock.Unlock()
	if !ok {
		return fmt.Errorf("no key [%s] on node [%s]'s backupData", key, node.getPort())
	}
	return nil
}

// helper functions for debugging

func (node *Node) PrintNodeInfo() {
	var suc, pre SingleNode
	node.get_successor(&suc)
	node.get_predecessor(&pre)
	logrus.Infof(">>>>>>>>>> node [%s]: successor [%s], predecessor [%s] <<<<<<<<<<", node.getPort(), suc.getPort(), pre.getPort())
}

func (node *Node) PrintFingers() {
	var f SingleNode
	var i uint
	for i = 0; i < M; i++ {
		node.get_finger_i(i, &f)
		logrus.Infof(">>>>>>>>>> finger [%d]: [%s] <<<<<<<<<<", i, f.getPort())
	}
}

// get port number

func (node *SingleNode) getPort() string {
	return getPortFromIP(node.Addr)
}

func getPortFromIP(ip string) string {
	return ip[strings.Index(ip, ":")+1:]
}
