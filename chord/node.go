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
	node.online.Store(true)
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
	node.online.Store(true)
	node.maintainChord()
	return true
}

/*Init method Quit() for interface dhtNode*/
func (node *Node) Quit() {
	logrus.Infof("Info <func Quit()> node [%s] quit", node.getPort())
	node.PrintNodeInfo()
	node.online.Store(false)
	node.server.TurnOff()
	var suc, pre SingleNode
	err := node.get_successor(&suc)
	if err != nil && suc.Addr != node.Addr {
		logrus.Warnf("Warning! <func Quit()> %v", err)
	}
	// fmt.Printf("node [%s] quit step 0\n", node.getPort())
	err = node.get_predecessor(&pre)
	if err != nil && pre.Addr != node.Addr {
		logrus.Warnf("Warning! <func Quit()> %v", err)
	}
	// fmt.Printf("node [%s] quit step 1\n", node.getPort())
	if pre.Addr != node.Addr && suc.Addr != node.Addr {
		if pre.Addr != "" {
			node.RemoteCall("tcp", pre.Addr, "RPC_Node.Set_successor", &suc, &struct{}{})
		} else {
			logrus.Errorf("Error <func Quit()> fail to set successor of node [%s]'s unknown predecessor", node.getPort())
		}
	}
	// fmt.Printf("node [%s] quit step 2\n", node.getPort())
	if suc.Addr != node.Addr {
		if pre.Addr != node.Addr {
			node.RemoteCall("tcp", suc.Addr, "RPC_Node.Set_predecessor", &pre, &struct{}{})
		}
		node_data, node_backup := make(map[string]string), make(map[string]string)
		node.getDataList(&node_data)
		node.getBackupDataList(&node_backup)
		node.RemoteCall("tcp", suc.Addr, "RPC_Node.DeleteBackupDataList", node_data, &struct{}{})
		node.RemoteCall("tcp", suc.Addr, "RPC_Node.PutDataList", node_data, &struct{}{})
		node.RemoteCall("tcp", suc.Addr, "RPC_Node.PutBackupDataList", node_backup, &struct{}{})
	}
	// fmt.Printf("node [%s] quit step 3\n", node.getPort())
	node.clear()
}

/*Init method ForceQuit() for interface dhtNode*/
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
	if !node.Ping(res.Addr) {
		err := fmt.Errorf("node [%s]'s successor is offline when trying to get it", node.getPort())
		return err
	}
	return nil
}

func (node *Node) get_predecessor(res *SingleNode) error {
	node.predecessorLock.RLock()
	*res = node.predecessor
	node.predecessorLock.RUnlock()
	if res.Addr == "" {
		err := fmt.Errorf("node [%s] does not know its predecessor", node.getPort())
		return err
	} else if !node.Ping(res.Addr) {
		err := fmt.Errorf("node [%s]'s predecessor is offline when trying to get it", node.getPort())
		return err
	}
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
		node.set_successor(&nSuc)
		node.set_finger_i(0, &nSuc)
		node_data := make(map[string]string)
		node.getDataList(&node_data)
		node.RemoteCall("tcp", nSuc.Addr, "RPC_Node.PutBackupDataList", node_data, &struct{}{})
		node.RemoteCall("tcp", suc.Addr, "RPC_Node.DeleteBackupDataList", node_data, &struct{}{})
		suc_data := make(map[string]string)
		node.RemoteCall("tcp", suc.Addr, "RPC_Node.GetDataList", struct{}{}, &suc_data)
		var id *big.Int
		for key := range suc_data {
			id = Hash(key)
			if in_range(id, nSuc.ID, suc.ID) || id.Cmp(suc.ID) == 0 {
				delete(suc_data, key)
			}
		}
		node.RemoteCall("tcp", nSuc.Addr, "RPC_Node.PutDataList", suc_data, &struct{}{})
		node.RemoteCall("tcp", suc.Addr, "RPC_Node.DeleteDataList", suc_data, &struct{}{})
	}
	err = node.get_successor(&suc)
	if err != nil {
		logrus.Warnf("Warning! <func stabilize()> %v", err)
		return err
	}
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
	err := node.get_predecessor(&pre)
	if err != nil {
		logrus.Warnf("Warning! <func notify()> %v", err)
	}
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

func (node *Node) update_predecessor() {
	logrus.Infof("Info <func update_predecessor()> update [%s]'s predecessor", node.getPort())
	var pre SingleNode
	err := node.get_predecessor(&pre)
	if err != nil {
		logrus.Warnf("Warning! <func update_predecessor()> %v", err)
	}
	if pre.Addr != "" && !node.Ping(pre.Addr) {
		logrus.Infof("Info <func update_predecessor()> [%s]'s predecessor offline", node.getPort())
		node.set_predecessor(&SingleNode{"", nil})
		// now responsible for backup data of the predecessor
		backup := make(map[string]string)
		node.getBackupDataList(&backup)
		node.backupDataLock.Lock()
		node.backupData = make(map[string]string)
		node.backupDataLock.Unlock()
		node.putDataList(backup)
		// How to handle backup of offline predecessor
		// maybe in stabilize() judge is the backup of successor empty and decide put data there or not
	}
	node.PrintNodeInfo()
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

func (node *Node) putData(pair Pair) error {
	node.dataLock.Lock()
	node.data[pair.Key] = pair.Value
	node.dataLock.Unlock()
	var suc SingleNode
	err := node.get_successor(&suc)
	if err != nil {
		logrus.Warnf("Warning! <func putData()> %v", err)
		return err
	}
	err = node.RemoteCall("tcp", suc.Addr, "RPC_Node.PutBackupData", pair, &struct{}{})
	if err != nil {
		logrus.Errorf("Error <func putData()> node [%s] call [%s] method [RPC_Node.PutBackupData] error: %v", node.getPort(), suc.getPort(), err)
		return err
	}
	return nil
}

func (node *Node) putDataList(dataList map[string]string) error {
	node.dataLock.Lock()
	for key, value := range dataList {
		node.data[key] = value
	}
	node.dataLock.Unlock()
	var suc SingleNode
	err := node.get_successor(&suc)
	if err != nil {
		logrus.Warnf("Warning! <func putDataList()> %v", err)
		return err
	}
	err = node.RemoteCall("tcp", suc.Addr, "RPC_Node.PutBackupDataList", dataList, &struct{}{})
	if err != nil {
		logrus.Errorf("Error <func putDataList()> node [%s] call [%s] method [RPC_Node.PutBackupDataList] error: %v", node.getPort(), suc.getPort(), err)
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

func (node *Node) putBackupDataList(backupDataList map[string]string) error {
	node.backupDataLock.Lock()
	for key, value := range backupDataList {
		node.backupData[key] = value
	}
	node.backupDataLock.Unlock()
	return nil
}

func (node *Node) getData(key string, value *string) error {
	node.dataLock.RLock()
	*value = node.data[key]
	node.dataLock.RUnlock()
	return nil
}

func (node *Node) getDataList(dataList *map[string]string) error {
	node.dataLock.RLock()
	*dataList = node.data
	node.dataLock.RUnlock()
	return nil
}

func (node *Node) getBackupDataList(backupDataList *map[string]string) error {
	node.backupDataLock.Lock()
	*backupDataList = node.backupData
	node.backupDataLock.Unlock()
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
	err := node.get_successor(&suc)
	if err != nil {
		logrus.Warnf("Warning! <func deletaData()> %v", err)
		return err
	}
	err = node.RemoteCall("tcp", suc.Addr, "RPC_Node.DeleteBackupData", key, &struct{}{})
	if err != nil {
		logrus.Errorf("Error <func deleteData()> node [%s] call [%s] method [RPC_Node.DeleteBackupData] error: %v", node.getPort(), suc.getPort(), err)
		return err
	}
	return nil
}

func (node *Node) deleteDataList(dataList map[string]string) error {
	node.dataLock.Lock()
	for key := range dataList {
		delete(node.data, key)
	}
	node.dataLock.Unlock()
	var suc SingleNode
	err := node.get_successor(&suc)
	if err != nil {
		logrus.Warnf("Warning! <func deleteDataList()> %v", err)
		return err
	}
	err = node.RemoteCall("tcp", suc.Addr, "RPC_Node.DeleteBackupDataList", dataList, &struct{}{})
	if err != nil {
		logrus.Errorf("Error <func deleteDataList()> node [%s] call [%s] method [RPC_Node.DeleteBackupDataList] error: %v", node.getPort(), suc.getPort(), err)
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

func (node *Node) deleteBackupDataList(backupDataList map[string]string) error {
	node.backupDataLock.Lock()
	for key := range backupDataList {
		delete(node.backupData, key)
	}
	node.backupDataLock.Unlock()
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
