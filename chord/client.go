package chord

import (
	"net"
	"net/rpc"
	"time"

	"github.com/sirupsen/logrus"
)

const (
	// dialTimes             = 3
	dialDuration = 500 * time.Millisecond
	// dialFailSleepDuration = 200 * time.Millisecond
)

func (node *Node) GetClient(netStr, addr string) (*rpc.Client, error) {
	var conn net.Conn
	var err error
	// for i := 0; i < dialTimes; i++ {
	conn, err = net.DialTimeout(netStr, addr, dialDuration)
	if err == nil {
		client := rpc.NewClient(conn)
		return client, nil
	}
	logrus.Infof("Info <func GetClient()> find node [%s] shutdown when dialing it", getPortFromIP(addr))
	// 	time.Sleep(dialFailSleepDuration)
	// }
	return nil, err
}

func (node *Node) RemoteCall(netStr, addr, method string, args, reply interface{}) error {
	client, err := node.GetClient(netStr, addr)
	if err != nil {
		logrus.Errorf("Error <func RemoteCall()> node [%s] getClient of node [%s] for method [%s] error: %v", node.getPort(), getPortFromIP(addr), method, err)
		return err
	}
	// logrus.Infof("Info <func RemoteCall()> node [%s] RemoteCall node [%s] method [%s]", node.getPort(), getPortFromIP(addr), method)
	err = client.Call(method, args, reply)
	if err != nil {
		client.Close()
		logrus.Errorf("Error <func RemoteCall()> node [%s] call method [%s] error: %v", node.getPort(), method, err)
	}
	return err
}
