package chord

import (
	"net"
	"net/rpc"
	"sync/atomic"

	"github.com/sirupsen/logrus"
)

type NodeServer struct {
	server   *rpc.Server
	listener net.Listener
	shutdown atomic.Bool
}

func (ns *NodeServer) Init(node *Node) {
	ns.server = rpc.NewServer()
	ns.server.Register(&RPC_Node{node})
}

func (ns *NodeServer) TurnOn(netStr, addr string) error {
	var err error
	ns.listener, err = net.Listen(netStr, addr)
	if err != nil {
		logrus.Errorf("Error <func TurnOn()> node [%s] listen error: %v", getPortFromIP(addr), err)
		return err
	}
	ns.shutdown.Store(false)
	go func() {
		err = ns.DialAccept()
		if ns.shutdown.Load() {
			logrus.Infof("Info <func TurnOn()> node [%s] shutdown", getPortFromIP(addr))
		} else {
			logrus.Errorf("Error <func TurnOn()> node [%s] accept error: %v", getPortFromIP(addr), err)
		}
	}()
	return nil
}

func (ns *NodeServer) DialAccept() error {
	for {
		conn, err := ns.listener.Accept()
		if err != nil {
			return err
		}
		go ns.server.ServeConn((conn))
	}
}

func (ns *NodeServer) TurnOff() {
	ns.listener.Close()
	ns.shutdown.Store(true)
}
