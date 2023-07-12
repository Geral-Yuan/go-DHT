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
	stabilizeInterval         = 200 * time.Millisecond
	fixFingersInterval        = 200 * time.Millisecond
	updatePredecessorInterval = 200 * time.Millisecond
	successorListLength       = 5
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
	err := node.RemoteCall("tcp", addr, "RPC_Node.Find_successor", node.ID, &suc)
	if err != nil {
		logrus.Errorf("Error <func Join()> node [%s] call [%s] method [RPC_Node.Find_successor] error: %v", node.getPort(), getPortFromIP(addr), err)
		return false
	}
	node.add_successor(suc)
	node.dataLock.Lock()
	node.RemoteCall("tcp", suc.Addr, "RPC_Node.TransferData", SingleNode{node.Addr, node.ID}, &node.data)
	node.dataLock.Unlock()
	node.online.Store(true)
	node.maintainChord()
	return true
}

/*Implement method Quit() for interface dhtNode*/
func (node *Node) Quit() {
	logrus.Infof("Info <func Quit()> node [%s] quit", node.getPort())
	node.PrintNodeInfo()
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
	// if pre.Addr != node.Addr && suc.Addr != node.Addr {
	// 	if pre.Addr != "" {
	// 		node.RemoteCall("tcp", pre.Addr, "RPC_Node.Set_successor", &suc, &struct{}{})
	// 	} else {
	// 		logrus.Errorf("Error <func Quit()> fail to set successor of node [%s]'s unknown predecessor", node.getPort())
	// 	}
	// }
	// if suc.Addr != node.Addr {
	// 	if pre.Addr != node.Addr {
	// 		node.RemoteCall("tcp", suc.Addr, "RPC_Node.Set_predecessor", &pre, &struct{}{})
	// 	}
	// 	var node_data, node_backup map[string]string
	// 	node.getDataList(&node_data)
	// 	node.getBackupDataList(&node_backup)
	// 	node.RemoteCall("tcp", suc.Addr, "RPC_Node.DeleteBackupDataList", node_data, &struct{}{})
	// 	node.RemoteCall("tcp", suc.Addr, "RPC_Node.PutDataList", node_data, &struct{}{})
	// 	node.RemoteCall("tcp", suc.Addr, "RPC_Node.PutBackupDataList", node_backup, &struct{}{})
	// }
	node.clear()
}

/*Implement method ForceQuit() for interface dhtNode*/
func (node *Node) ForceQuit() {
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

/*Implement method Get() for interface dhtNode*/
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

/*Implement method Delete() for interface dhtNode*/
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
	var i uint
	for i = 0; i < successorListLength; i++ {
		node.successorListLock.RLock()
		*res = node.successorList[i]
		node.successorListLock.RUnlock()
		if res.Addr != "" && node.Ping(res.Addr) {
			return nil
		}
	}
	*res = SingleNode{}
	return fmt.Errorf("no node in [%s]'s successorList is online", node.getPort())
}

func (node *Node) get_successorList(res *[successorListLength]SingleNode) error {
	node.successorListLock.RLock()
	*res = node.successorList
	node.successorListLock.RUnlock()
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
	node.successorListLock.Lock()
	node.successorList[0] = *n
	node.successorListLock.Unlock()
	return nil
}

func (node *Node) add_successor(suc SingleNode) error {
	node.set_successor(&suc)
	node.set_finger_i(0, &suc)
	var suc_successorList [successorListLength]SingleNode
	node.RemoteCall("tcp", suc.Addr, "RPC_Node.Get_successorList", struct{}{}, &suc_successorList)
	var i uint
	node.successorListLock.Lock()
	for i = 1; i < successorListLength; i++ {
		node.successorList[i] = suc_successorList[i-1]
	}
	node.successorListLock.Unlock()
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

func (node *Node) clear() {
	node.dataLock.Lock()
	node.data = make(map[string]string)
	node.dataLock.Unlock()
	node.backupDataLock.Lock()
	node.backupData = make(map[string]string)
	node.backupDataLock.Unlock()
}

// find the successor of id
func (node *Node) find_successor(id *big.Int, res *SingleNode) error {
	// logrus.Infof("Info <func find_successor()> node [%s] find successor of [%v]", node.getPort(), id)
	if !node.online.Load() {
		logrus.Infof("Info <func find_successor()> node [%s] offline", node.getPort())
	}
	var suc SingleNode
	err := node.get_successor(&suc)
	if err != nil {
		logrus.Warnf("Warning! <func find_successor()> %v", err)
		return err
	}
	if in_range(id, node.ID, suc.ID) || (id.Cmp(suc.ID) == 0) {
		*res = suc
		logrus.Infof("Info <func find_successor()> node [%s] find successor [%s]", node.getPort(), suc.getPort())
		return nil
	}
	prec_finger := node.closest_preceding_finger(id)
	if prec_finger.Addr == node.Addr {
		*res = suc
		return nil
	}
	// logrus.Infof("Info <func find_successor()> node [%s] remotecall [%s][%v] method [RPC_Node.Find_successor]", node.getPort(), prec_finger.getPort(), node.Ping(prec_finger.Addr))
	return node.RemoteCall("tcp", prec_finger.Addr, "RPC_Node.Find_successor", id, res)
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
	logrus.Infof("Info <func stabilize()> node [%s] stabilize", node.getPort())
	var suc, nSuc SingleNode
	err := node.get_successor(&suc)
	if err != nil {
		logrus.Warnf("Warning! <func stabilize()> %v", err)
		return err
	}
	err = node.RemoteCall("tcp", suc.Addr, "RPC_Node.Get_predecessor", struct{}{}, &nSuc)
	if err != nil {
		logrus.Errorf("Error <func stabilize()> node [%s] call [%s] method [RPC_Node.Get_predecessor] error: %v", node.getPort(), suc.getPort(), err)
		return err
	}
	if nSuc.Addr != "" && in_range(nSuc.ID, node.ID, suc.ID) {
		suc = nSuc
	}
	node.add_successor(suc)
	err = node.RemoteCall("tcp", suc.Addr, "RPC_Node.Notify", SingleNode{node.Addr, node.ID}, &struct{}{})
	if err != nil {
		logrus.Errorf("Error <func stabilize()> node [%s] call [%s] method [RPC_Node.Notify] error: %v", node.getPort(), suc.getPort(), err)
		return err
	}
	node.PrintNodeInfo()
	return nil
}

func (node *Node) notify(n SingleNode) error {
	if !node.Ping(n.Addr) {
		return nil
	}
	var pre SingleNode
	node.get_predecessor(&pre)
	if pre.Addr == "" || in_range(n.ID, pre.ID, node.ID) {
		node.set_predecessor(&n)
		var pre_data map[string]string
		node.RemoteCall("tcp", n.Addr, "RPC_Node.GetDataList", struct{}{}, &pre_data)
		node.backupDataLock.Lock()
		node.backupData = pre_data
		node.backupDataLock.Unlock()
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

func (node *Node) update_predecessor() error {
	logrus.Infof("Info <func update_predecessor()> update [%s]'s predecessor", node.getPort())
	var pre SingleNode
	node.get_predecessor(&pre)
	if pre.Addr != "" && !node.Ping(pre.Addr) {
		logrus.Infof("Info <func update_predecessor()> [%s]'s predecessor offline", node.getPort())
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
	node.PrintNodeInfo()
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

func (node *Node) isOnline() error {
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
