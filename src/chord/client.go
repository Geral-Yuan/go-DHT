package chord

import (
	"net/rpc"

	"github.com/sirupsen/logrus"
)

func (node *Node) GetClient(netStr, addr string) (*rpc.Client, error) {
	client, err := rpc.Dial(netStr, addr)
	if err != nil {
		return nil, err
	}
	return client, nil
}

func (node *Node) RemoteCall(netStr, addr, method string, args, reply interface{}) error {
	client, err := node.GetClient(netStr, addr)
	if err != nil {
		// logrus.Errorf("Error <func RemoteCall()> node [%s] getClient of node [%s] for method [%s] error: %v", node.getPort(), getPortFromIP(addr), method, err)
		return err
	}
	// logrus.Infof("Info <func RemoteCall()> node [%s] RemoteCall node [%s] method [%s]", node.getPort(), getPortFromIP(addr), method)
	err = client.Call(method, args, reply)
	if err != nil {
		client.Close()
		logrus.Errorf("Error <func RemoteCall()> node [%s] call method [%s] error: %v", node.getPort(), method, err)
	}
	if client != nil {
		defer client.Close()
	}
	return err
}
