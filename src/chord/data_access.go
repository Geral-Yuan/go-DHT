package chord

import (
	"fmt"
	"math/big"

	"github.com/sirupsen/logrus"
)

func (node *Node) putData(pair Pair) error {
	node.dataLock.Lock()
	node.data[pair.Key] = pair.Value
	node.dataLock.Unlock()
	logrus.Infof("Info <func putData()> put key [%s] on node [%s]'s data", pair.Key, node.getPort())
	var suc SingleNode
	err := node.get_successor(&suc)
	if err != nil {
		logrus.Warnf("Warning! <func putData()> %v", err)
		return err
	}
	err = node.RemoteCall("tcp", suc.Addr, "RPC_Node.PutBackupData", pair, &struct{}{})
	if err != nil {
		logrus.Errorf("Error <func putData()> node [%s] call [%s] method [RPC_Node.PutBackupData] error: %v", node.getPort(), suc.getPort(), err)
		return err
	}
	return nil
}

func (node *Node) putDataList(dataList map[string]string) error {
	node.dataLock.Lock()
	for key, value := range dataList {
		node.data[key] = value
	}
	node.dataLock.Unlock()
	for key := range dataList {
		logrus.Infof("Info <func putDataList()> put key [%s] on node [%s]'s data", key, node.getPort())
	}
	var suc SingleNode
	err := node.get_successor(&suc)
	if err != nil {
		logrus.Warnf("Warning! <func putDataList()> %v", err)
		return err
	}
	err = node.RemoteCall("tcp", suc.Addr, "RPC_Node.PutBackupDataList", dataList, &struct{}{})
	if err != nil {
		logrus.Errorf("Error <func putDataList()> node [%s] call [%s] method [RPC_Node.PutBackupDataList] error: %v", node.getPort(), suc.getPort(), err)
		return err
	}
	return nil
}

func (node *Node) putBackupData(pair Pair) error {
	node.backupDataLock.Lock()
	node.backupData[pair.Key] = pair.Value
	node.backupDataLock.Unlock()
	logrus.Infof("Info <func putBackupData()> put key [%s] on node [%s]'s backupData", pair.Key, node.getPort())
	return nil
}

func (node *Node) putBackupDataList(backupDataList map[string]string) error {
	node.backupDataLock.Lock()
	for key, value := range backupDataList {
		node.backupData[key] = value
	}
	node.backupDataLock.Unlock()
	for key := range backupDataList {
		logrus.Infof("Info <func putbackupDataList()> put key [%s] on node [%s]'s backupData", key, node.getPort())
	}
	return nil
}

func (node *Node) getData(key string, value *string) error {
	var ok bool
	node.dataLock.RLock()
	*value, ok = node.data[key]
	node.dataLock.RUnlock()
	if !ok {
		logrus.Infof("Info <func getData()> no key [%s] on node [%s]'s data", key, node.getPort())
		return fmt.Errorf("no key [%s] on node [%s]'s data when getting", key, node.getPort())
	}
	return nil
}

func (node *Node) getDataList(dataList *map[string]string) error {
	node.dataLock.RLock()
	*dataList = node.data
	node.dataLock.RUnlock()
	return nil
}

func (node *Node) getBackupDataList(backupDataList *map[string]string) error {
	node.backupDataLock.Lock()
	*backupDataList = node.backupData
	node.backupDataLock.Unlock()
	return nil
}

func (node *Node) deleteData(key string) error {
	node.dataLock.Lock()
	_, ok := node.data[key]
	delete(node.data, key)
	node.dataLock.Unlock()
	if !ok {
		return fmt.Errorf("no key [%s] on node [%s]'s data", key, node.getPort())
	}
	logrus.Infof("Info <func deleteData()> delete key [%s] on node [%s]'s data", key, node.getPort())
	var suc SingleNode
	err := node.get_successor(&suc)
	if err != nil {
		logrus.Warnf("Warning! <func deletaData()> %v", err)
		return err
	}
	err = node.RemoteCall("tcp", suc.Addr, "RPC_Node.DeleteBackupData", key, &struct{}{})
	if err != nil {
		logrus.Errorf("Error <func deleteData()> node [%s] call [%s] method [RPC_Node.DeleteBackupData] error: %v", node.getPort(), suc.getPort(), err)
		return err
	}
	return nil
}

func (node *Node) deleteDataList(dataList map[string]string) error {
	for key := range dataList {
		node.dataLock.RLock()
		_, ok := node.data[key]
		node.dataLock.RUnlock()
		if !ok {
			logrus.Errorf("Error <func deleteDataList()> no key [%s] on node [%s]'s data", key, node.getPort())
		} else {
			logrus.Infof("Info <func deleteDataList()> delete key [%s] on node [%s]'s data", key, node.getPort())
			node.dataLock.Lock()
			delete(node.data, key)
			node.dataLock.Unlock()
		}
	}
	var suc SingleNode
	err := node.get_successor(&suc)
	if err != nil {
		logrus.Warnf("Warning! <func deleteDataList()> %v", err)
		return err
	}
	err = node.RemoteCall("tcp", suc.Addr, "RPC_Node.DeleteBackupDataList", dataList, &struct{}{})
	if err != nil {
		logrus.Errorf("Error <func deleteDataList()> node [%s] call [%s] method [RPC_Node.DeleteBackupDataList] error: %v", node.getPort(), suc.getPort(), err)
		return err
	}
	return nil
}

func (node *Node) deleteBackupData(key string) error {
	node.backupDataLock.Lock()
	_, ok := node.backupData[key]
	delete(node.backupData, key)
	node.backupDataLock.Unlock()
	if !ok {
		return fmt.Errorf("no key [%s] on node [%s]'s backupData", key, node.getPort())
	}
	logrus.Infof("Info <func deleteBackupData()> delete key [%s] on node [%s]'s backupData", key, node.getPort())
	return nil
}

func (node *Node) deleteBackupDataList(backupDataList map[string]string) error {
	for key := range backupDataList {
		node.backupDataLock.RLock()
		_, ok := node.backupData[key]
		node.backupDataLock.RUnlock()
		if !ok {
			logrus.Errorf("Error <func deleteBackupDataList()> no key [%s] on node [%s]'s BackupData", key, node.getPort())
		} else {
			logrus.Infof("Info <func deleteBackupDataList()> delete key [%s] on node [%s]'s BackupData", key, node.getPort())
			node.backupDataLock.Lock()
			delete(node.backupData, key)
			node.backupDataLock.Unlock()
		}
	}
	return nil
}

func (node *Node) transferData(pre SingleNode, pre_data *map[string]string) error {
	node.backupData = make(map[string]string)
	var id *big.Int
	var node_data map[string]string
	node.getDataList(&node_data)
	for key, value := range node_data {
		id = Hash(key)
		if !(in_range(id, pre.ID, node.ID) || id.Cmp(node.ID) == 0) {
			(*pre_data)[key] = value
			node.backupDataLock.Lock()
			node.backupData[key] = value
			node.backupDataLock.Unlock()
			logrus.Infof("Info <func transferData()> put key [%s] on node [%s]'s backupData", key, node.getPort())
			node.dataLock.Lock()
			delete(node.data, key)
			node.dataLock.Unlock()
			logrus.Infof("Info <func transferData()> delete key [%s] on node [%s]'s data", key, node.getPort())
		}
	}
	var suc SingleNode
	node.get_successor(&suc)
	node.RemoteCall("tcp", suc.Addr, "RPC_Node.DeleteBackupDataList", node.backupData, &struct{}{})
	node.set_predecessor(&pre)
	return nil
}
