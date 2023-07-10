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

func (rpc *RPC_Node) Set_successor(n *SingleNode, _ *struct{}) error {
	return rpc.node_ptr.set_successor(n)
}

func (rpc *RPC_Node) Set_predecessor(n *SingleNode, _ *struct{}) error {
	return rpc.node_ptr.set_predecessor(n)
}

func (rpc *RPC_Node) Stablilize(_ struct{}, _ *struct{}) error {
	return rpc.node_ptr.stabilize()
}

func (rpc *RPC_Node) Notify(n SingleNode, _ *struct{}) error {
	return rpc.node_ptr.notify(n)
}

func (rpc *RPC_Node) IsOnline(_ struct{}, _ *struct{}) error {
	return rpc.node_ptr.isOnline()
}

func (rpc *RPC_Node) PutData(pair Pair, _ *struct{}) error {
	return rpc.node_ptr.putData(pair)
}

func (rpc *RPC_Node) PutDataList(dataList map[string]string, _ *struct{}) error {
	return rpc.node_ptr.putDataList(dataList)
}

func (rpc *RPC_Node) PutBackupData(pair Pair, _ *struct{}) error {
	return rpc.node_ptr.putBackupData(pair)
}

func (rpc *RPC_Node) PutBackupDataList(backupList map[string]string, _ *struct{}) error {
	return rpc.node_ptr.putBackupDataList(backupList)
}

func (rpc *RPC_Node) GetData(key string, value *string) error {
	return rpc.node_ptr.getData(key, value)
}

func (rpc *RPC_Node) GetDataList(_ struct{}, dataList *map[string]string) error {
	return rpc.node_ptr.getDataList(dataList)
}

func (rpc *RPC_Node) DeleteData(key string, _ *struct{}) error {
	return rpc.node_ptr.deleteData(key)
}

func (rpc *RPC_Node) DeleteDataList(dataList map[string]string, _ *struct{}) error {
	return rpc.node_ptr.deleteBackupDataList(dataList)
}

func (rpc *RPC_Node) DeleteBackupData(key string, _ *struct{}) error {
	return rpc.node_ptr.deleteBackupData(key)
}

func (rpc *RPC_Node) DeleteBackupDataList(backupDataList map[string]string, _ *struct{}) error {
	return rpc.node_ptr.deleteBackupDataList(backupDataList)
}
