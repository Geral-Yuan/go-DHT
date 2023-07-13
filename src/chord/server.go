package chord

import (
	"net"
	"net/rpc"
	"sync"
	"sync/atomic"

	"github.com/sirupsen/logrus"
)

type NodeServer struct {
	server          *rpc.Server
	listener        net.Listener
	shutdown        atomic.Bool
	connections     map[net.Conn]struct{}
	connectionsLock sync.Mutex
}

func (ns *NodeServer) Init(node *Node) {
	ns.server = rpc.NewServer()
	ns.connections = make(map[net.Conn]struct{})
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
		err = ns.DialAccept(addr)
	}()
	return nil
}

func (ns *NodeServer) DialAccept(addr string) error {
	for {
		conn, err := ns.listener.Accept()
		if err != nil {
			if ns.shutdown.Load() {
				logrus.Infof("Info <func DialAccept()> node [%s] shutdown", getPortFromIP(addr))
			} else {
				logrus.Errorf("Error <func DialAccept()> node [%s] accept error: %v", getPortFromIP(addr), err)
			}
			return err
		}
		ns.connectionsLock.Lock()
		ns.connections[conn] = struct{}{}
		ns.connectionsLock.Unlock()
		go func() {
			ns.server.ServeConn((conn))
			ns.connectionsLock.Lock()
			delete(ns.connections, conn)
			ns.connectionsLock.Unlock()
		}()
	}
}

func (ns *NodeServer) TurnOff() {
	ns.shutdown.Store(true)
	ns.listener.Close()
	ns.connectionsLock.Lock()
	for conn := range ns.connections {
		conn.Close()
		delete(ns.connections, conn)
	}
	ns.connectionsLock.Unlock()
}
