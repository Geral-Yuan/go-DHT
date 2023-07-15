package chord

import (
	"fmt"
)

func (node *Node) isOnline() error {
	return nil
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
		// else {
		// 	logrus.Infof("Info <func get_successor()> node [%s] fail to ping node [%s]", node.getPort(), res.getPort())
		// }
	}
	*res = SingleNode{"", nil}
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
	if node.Ping(res.Addr) {
		return nil
	} else {
		return fmt.Errorf("node [%s]'s predecessor offline", node.getPort())
	}
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
	if !node.Ping(suc.Addr) {
		return fmt.Errorf("find [%s] offline when adding it to node [%s]'s successorList", suc.getPort(), node.getPort())
	}
	node.set_successor(&suc)
	node.set_finger_i(0, &suc)
	var suc_successorList [successorListLength]SingleNode
	err := node.RemoteCall("tcp", suc.Addr, "RPC_Node.Get_successorList", struct{}{}, &suc_successorList)
	if err != nil {
		return err
	}
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
