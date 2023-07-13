package chord

import (
	"strings"

	"github.com/sirupsen/logrus"
)

// print information for debugging

func (node *Node) PrintNodeInfo() {
	var sucList [successorListLength]SingleNode
	var pre SingleNode
	node.successorListLock.RLock()
	node.get_successorList(&sucList)
	node.successorListLock.RUnlock()
	// node.get_successor(&suc)
	node.get_predecessor(&pre)
	logrus.Infof("node [%s]: successorList [%s][%s][%s][%s][%s], predecessor [%s]", node.getPort(), sucList[0].getPort(), sucList[1].getPort(), sucList[2].getPort(), sucList[3].getPort(), sucList[4].getPort(), pre.getPort())
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
