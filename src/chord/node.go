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
	stabilizeInterval         = 200 * time.Millisecond
	fixFingersInterval        = 200 * time.Millisecond
	updatePredecessorInterval = 200 * time.Millisecond
	successorListLength       = 10
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
	online            atomic.Bool
	server            NodeServer
	data              map[string]string
	dataLock          sync.RWMutex
	backupData        map[string]string
	backupDataLock    sync.RWMutex
	successorList     [successorListLength]SingleNode
	successorListLock sync.RWMutex
	predecessor       SingleNode
	predecessorLock   sync.RWMutex
	finger            [M]SingleNode
	fingerLock        sync.RWMutex
	fingerFixIndex    uint
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
	node.fingerFixIndex = 0
	node.server.Init(node)
	node.online.Store(false)
}

// followings are methods for interface dhtNode

/*Implement method Run() for interface dhtNode*/
func (node *Node) Run() {
	err := node.server.TurnOn("tcp", node.Addr)
	if err != nil {
		logrus.Errorf("Error <func Run()> turn on node [%s] error: %v\n", node.getPort(), err)
	}
}

/*Implement method Create() for interface dhtNode*/
func (node *Node) Create() {
	logrus.Infof("Info <func Create()> node [%s] create a chord", node.getPort())
	node.successorList[0] = SingleNode{node.Addr, node.ID}
	node.predecessor = SingleNode{node.Addr, node.ID}
	var i uint
	for i = 0; i < M; i++ {
		node.finger[i] = SingleNode{node.Addr, node.ID}
	}
	node.online.Store(true)
	node.maintainChord()
}

/*Implement method Join() for interface dhtNode*/
func (node *Node) Join(addr string) bool {
	logrus.Infof("Info <func Join()> node [%s] join node [%s]", node.getPort(), getPortFromIP(addr))
	var suc SingleNode
	err := node.RemoteCall("tcp", addr, "RPC_Node.Find_successor", ArgNode{node.ID, 0}, &suc)
	if err != nil {
		logrus.Errorf("Error <func Join()> node [%s] call [%s] method [RPC_Node.Find_successor] error: %v", node.getPort(), getPortFromIP(addr), err)
		return false
	}
	node.add_successor(suc)
	node_data := make(map[string]string)
	node.RemoteCall("tcp", suc.Addr, "RPC_Node.TransferData", SingleNode{node.Addr, node.ID}, &node_data)
	node.dataLock.Lock()
	node.data = node_data
	node.dataLock.Unlock()
	// node.dataLock.RLock()
	// for key := range node.data {
	// 	logrus.Infof("Info <func Join()> put key [%s] on node [%s]'s data", key, node.getPort())
	// }
	// node.dataLock.RUnlock()
	node.online.Store(true)
	node.maintainChord()
	return true
}

/*Implement method Quit() for interface dhtNode*/
func (node *Node) Quit() {
	if !node.online.Load() {
		logrus.Infof("Info <func Quit()> node [%s] already quit", node.getPort())
		return
	}
	logrus.Infof("Info <func Quit()> node [%s] quit", node.getPort())
	// node.PrintNodeInfo()
	node.online.Store(false)
	node.server.TurnOff() // Maybe here is not best
	var suc, pre SingleNode
	err := node.get_successor(&suc)
	if err != nil && suc.Addr != node.Addr {
		logrus.Warnf("Warning! <func Quit()> %v", err)
	}
	node.RemoteCall("tcp", suc.Addr, "RPC_Node.Update_predecessor", struct{}{}, &struct{}{})
	node.get_predecessor(&pre)
	node.RemoteCall("tcp", pre.Addr, "RPC_Node.Stabilize", struct{}{}, &struct{}{})
	node.clear()
}

/*Implement method ForceQuit() for interface dhtNode*/
func (node *Node) ForceQuit() {
	if !node.online.Load() {
		logrus.Infof("Info <func ForceQuit()> node [%s] already quit", node.getPort())
		return
	}
	logrus.Infof("Info <func ForceQuit()> node [%s] force quit", node.getPort())
	// node.PrintNodeInfo()
	node.online.Store(false)
	node.server.TurnOff()
	node.clear()
}

func (node *Node) Ping(addr string) bool {
	// logrus.Infof("Info <func Ping()> node [%s] ping [%s]", node.getPort(), getPortFromIP(addr))
	if addr == node.Addr {
		return node.online.Load()
	}
	err := node.RemoteCall("tcp", addr, "RPC_Node.IsOnline", struct{}{}, &struct{}{})
	if err != nil {
		return false
	}
	return true
}

/*Implement method Put() for interface dhtNode*/
func (node *Node) Put(key string, value string) bool {
	logrus.Infof("Info <func Put()> node [%s] put key [%s]", node.getPort(), key)
	id := Hash(key)
	var suc SingleNode
	// logrus.Infof("Info <func Put()> node [%s] call find_successor", node.getPort())
	node.find_successor(ArgNode{id, 0}, &suc)
	// logrus.Infof("Info <func Put()> node [%s] get [%s] from find_successor", node.getPort(), suc.getPort())
	err := node.RemoteCall("tcp", suc.Addr, "RPC_Node.PutData", Pair{key, value}, &struct{}{})
	if err != nil {
		logrus.Errorf("Error <func Put()> node [%s] call [%s] method [RPC_Node.PutData] error: %v", node.getPort(), suc.getPort(), err)
		return false
	}
	return true
}

/*Implement method Get() for interface dhtNode*/
func (node *Node) Get(key string) (bool, string) {
	logrus.Infof("Info <func Get()> node [%s] get key [%s]", node.getPort(), key)
	id := Hash(key)
	var suc SingleNode
	// logrus.Infof("Info <func Get()> node [%s] call find_successor", node.getPort())
	node.find_successor(ArgNode{id, 0}, &suc)
	// logrus.Infof("Info <func Get()> node [%s] get [%s] from find_successor", node.getPort(), suc.getPort())
	node.update_predecessor()
	var value string
	err := node.RemoteCall("tcp", suc.Addr, "RPC_Node.GetData", key, &value)
	if err != nil {
		logrus.Errorf("Error <func Get()> node [%s] call [%s] method [RPC_Node.GetData] error: %v", node.getPort(), suc.getPort(), err)
		return false, ""
	}
	return true, value
}

/*Implement method Delete() for interface dhtNode*/
func (node *Node) Delete(key string) bool {
	logrus.Infof("Info <func Delete()> node [%s] delete key [%s]", node.getPort(), key)
	id := Hash(key)
	var suc SingleNode
	// logrus.Infof("Info <func Delete()> node [%s] call find_successor", node.getPort())
	node.find_successor(ArgNode{id, 0}, &suc)
	// logrus.Infof("Info <func Delete()> node [%s] get [%s] from find_successor", node.getPort(), suc.getPort())
	err := node.RemoteCall("tcp", suc.Addr, "RPC_Node.DeleteData", key, &struct{}{})
	if err != nil {
		logrus.Errorf("Error <func Delete()> node [%s] call [%s] method [RPC_Node.DeleteData] error: %v", node.getPort(), suc.getPort(), err)
		return false
	}
	return true
}

// following are helper functions for interface methods and RPC methods

func (node *Node) clear() {
	node.dataLock.Lock()
	node.data = make(map[string]string)
	node.dataLock.Unlock()
	node.backupDataLock.Lock()
	node.backupData = make(map[string]string)
	node.backupDataLock.Unlock()
}

type ArgNode struct {
	ID    *big.Int
	Depth uint
}

// find the successor of id
func (node *Node) find_successor(n ArgNode, res *SingleNode) error {
	// logrus.Infof("Info <func find_successor()> node [%s] find successor of [%d] depth [%d]", node.getPort(), n.ID, n.Depth)
	if !node.online.Load() {
		logrus.Infof("Info <func find_successor()> node [%s] offline", node.getPort())
	}
	var suc SingleNode
	err := node.get_successor(&suc)
	if err != nil {
		logrus.Warnf("Warning! <func find_successor()> node [%s] find successor of [%d] %v", node.getPort(), n.ID, err)
		return err
	}
	if in_range(n.ID, node.ID, suc.ID) || (n.ID.Cmp(suc.ID) == 0) {
		*res = suc
		// logrus.Infof("Info <func find_successor()> node [%s] find [%s] successor of [%d]", node.getPort(), suc.getPort(), n.ID)
		return nil
	}
	prec_finger := node.closest_preceding_finger(n.ID)
	if prec_finger.Addr == node.Addr {
		*res = suc
		// logrus.Infof("Info <func find_successor()> node [%s] return its successor [%s] for successor of [%d]", node.getPort(), suc.getPort(), n.ID)
		return nil
	}
	// logrus.Infof("Info <func find_successor()> node [%s] remotecall [%s][%v] method [RPC_Node.Find_successor]", node.getPort(), prec_finger.getPort(), node.Ping(prec_finger.Addr))
	return node.RemoteCall("tcp", prec_finger.Addr, "RPC_Node.Find_successor", ArgNode{n.ID, n.Depth + 1}, res)
}

func (node *Node) closest_preceding_finger(id *big.Int) SingleNode {
	if !node.online.Load() {
		logrus.Infof("Info <func closest_preceding_finger()> node [%s] offline", node.getPort())
	}
	var i uint
	for i = M - 1; i < M; i-- {
		var f SingleNode
		node.get_finger_i(i, &f)
		if f.Addr == "" {
			continue
		}
		if !node.Ping(f.Addr) {
			logrus.Infof("Info <func cloest_preceding_finger()> node [%s] fail to ping node [%s]", node.getPort(), f.getPort())
			node.set_finger_i(i, &SingleNode{"", nil})
			continue
		}
		if in_range(f.ID, node.ID, id) {
			return f
		}
	}
	var suc SingleNode
	err := node.get_successor(&suc)
	if err != nil {
		logrus.Warnf("Warning! <func closest_preceding_finger()> %v", err)
	}
	return suc
}

func (node *Node) stabilize() error {
	// logrus.Infof("Info <func stabilize()> node [%s] stabilize", node.getPort())
	var suc, nSuc SingleNode
	err := node.get_successor(&suc)
	if err != nil {
		logrus.Warnf("Warning! <func stabilize()> %v", err)
		return err
	}
	err = node.RemoteCall("tcp", suc.Addr, "RPC_Node.Get_predecessor", struct{}{}, &nSuc)
	if err != nil {
		logrus.Warnf("Warning <func stabilize()> node [%s] call [%s] method [RPC_Node.Get_predecessor] error: %v", node.getPort(), suc.getPort(), err)
	}
	if nSuc.Addr != "" && in_range(nSuc.ID, node.ID, suc.ID) {
		suc = nSuc
	}
	err = node.add_successor(suc)
	if err != nil {
		logrus.Warnf("Warning! <func stabilize()> %v", err)
		return nil
	}
	err = node.RemoteCall("tcp", suc.Addr, "RPC_Node.Notify", SingleNode{node.Addr, node.ID}, &struct{}{})
	if err != nil {
		logrus.Errorf("Error <func stabilize()> node [%s] call [%s] method [RPC_Node.Notify] error: %v", node.getPort(), suc.getPort(), err)
		return err
	}
	// node.PrintNodeInfo()
	return nil
}

func (node *Node) notify(n SingleNode) error {
	if !node.Ping(n.Addr) {
		logrus.Infof("Info <func notify()> node [%s] fail to ping node [%s]", node.getPort(), n.getPort())
		return nil
	}
	var pre SingleNode
	node.get_predecessor(&pre)
	if pre.Addr == "" || in_range(n.ID, pre.ID, node.ID) {
		node.set_predecessor(&n)
		var pre_data map[string]string
		err := node.RemoteCall("tcp", n.Addr, "RPC_Node.GetDataList", struct{}{}, &pre_data)
		if err != nil {
			logrus.Warnf("Warning! <func notify()> fail to get data of node [%s]'s predecessor [%s]", node.getPort(), n.getPort())
			pre_data = make(map[string]string)
		}
		node.backupDataLock.Lock()
		node.backupData = pre_data
		node.backupDataLock.Unlock()
	}
	return nil
}

func (node *Node) fix_fingers() {
	// logrus.Infof("Info <func fix_fingers()> fix node [%s]'s fingers", node.getPort())
	var f, nf SingleNode
	node.find_successor(ArgNode{calcID(node.ID, node.fingerFixIndex), 0}, &nf)
	node.get_finger_i(node.fingerFixIndex, &f)
	if nf.Addr != f.Addr {
		node.set_finger_i(node.fingerFixIndex, &nf)
		// logrus.Infof("Info <func fix_fingers()> set [%s]'s %dth finger as [%s]", node.getPort(), node.fingerFixIndex, nf.getPort())
	}
	node.fingerFixIndex = (node.fingerFixIndex + 1) % M
}

func (node *Node) update_predecessor() error {
	// logrus.Infof("Info <func update_predecessor()> update node [%s]'s predecessor", node.getPort())
	var pre SingleNode
	node.get_predecessor(&pre)
	if pre.Addr != "" && !node.Ping(pre.Addr) {
		logrus.Infof("Info <func update_predecessor()> node [%s] fail to ping node [%s]", node.getPort(), pre.getPort())
		logrus.Infof("Info <func update_predecessor()> node [%s]'s predecessor offline", node.getPort())
		node.set_predecessor(&SingleNode{"", nil})
		// now responsible for backup data of the predecessor
		var backup map[string]string
		node.getBackupDataList(&backup)
		node.backupDataLock.Lock()
		node.backupData = make(map[string]string)
		node.backupDataLock.Unlock()
		node.putDataList(backup)
		// How to handle backup of offline predecessor
		// maybe in stabilize() judge is the backup of successor empty and decide put data there or not
	}
	// node.PrintNodeInfo()
	return nil
}

func (node *Node) maintainChord() {
	go func() {
		for node.online.Load() {
			node.stabilize()
			time.Sleep(stabilizeInterval)
		}
	}()
	go func() {
		for node.online.Load() {
			node.fix_fingers()
			time.Sleep(fixFingersInterval)
		}
	}()
	go func() {
		for node.online.Load() {
			node.update_predecessor()
			time.Sleep(updatePredecessorInterval)
		}
	}()
}
