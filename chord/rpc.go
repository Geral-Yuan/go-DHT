package chord

import "math/big"

type RPC_Node struct {
	node_ptr *Node
}

func (rpc *RPC_Node) Find_successor(id *big.Int, res *SingleNode) error {
	return rpc.node_ptr.find_successor(id, res)
}

func (rpc *RPC_Node) Get_predecessor(_ struct{}, res *SingleNode) error {
	return rpc.node_ptr.get_predecessor(res)
}

func (rpc *RPC_Node) Stablilize(_ struct{}, _ *struct{}) error {
	return rpc.node_ptr.stablilize()
}

func (rpc *RPC_Node) Notify(n SingleNode, _ *struct{}) error {
	return rpc.node_ptr.notify(n)
}
