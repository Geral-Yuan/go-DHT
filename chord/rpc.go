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

func (rpc *RPC_Node) PutData(pair Pair, _ *struct{}) error {
	return rpc.node_ptr.putData(pair)
}

func (rpc *RPC_Node) PutBackupData(pair Pair, _ *struct{}) error {
	return rpc.node_ptr.putBackupData(pair)
}

func (rpc *RPC_Node) GetData(key string, value *string) error {
	return rpc.node_ptr.getData(key, value)
}

func (rpc *RPC_Node) DeleteData(key string, _ *struct{}) error {
	return rpc.node_ptr.deleteData(key)
}

func (rpc *RPC_Node) DeleteBackupData(key string, _ *struct{}) error {
	return rpc.node_ptr.deleteBackupData(key)
}
