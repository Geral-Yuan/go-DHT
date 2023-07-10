package chord

import (
	"net"
	"net/rpc"
	"time"

	"github.com/sirupsen/logrus"
)

const (
	dialTimes             = 3
	dialDuration          = 500 * time.Millisecond
	dialFailSleepDuration = time.Second
)

func GetClient(netStr, addr string) (*rpc.Client, error) {
	var conn net.Conn
	var err error
	for i := 0; i < dialTimes; i++ {
		conn, err = net.DialTimeout(netStr, addr, dialDuration)
		if err == nil {
			client := rpc.NewClient(conn)
			return client, nil
		}
		logrus.Errorf("Error <func GetClient()> dial node [%s] error: %v", getPortFromIP(addr), err)
		time.Sleep(dialFailSleepDuration)
	}
	return nil, err
}

func (node *Node) RemoteCall(netStr, addr, method string, args, reply interface{}) error {
	client, err := GetClient(netStr, addr)
	if err != nil {
		logrus.Errorf("Error <func RemoteCall()> getClient of node [%s] error: %v", getPortFromIP(addr), err)
		return err
	}
	logrus.Infof("Info <func RemoteCall()> node [%s] RemoteCall node [%s] method [%s]", node.getPort(), getPortFromIP(addr), method)
	err = client.Call(method, args, reply)
	if err != nil {
		client.Close()
		logrus.Errorf("Error <func RemoteCall()> node [%s] call method [%s] error: %v", node.getPort(), method, err)
	}
	return err
}
