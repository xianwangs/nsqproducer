package nsqproducer

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
)

// NSQLookupProducer NSQNodes nsqlookup /nodes接口获取到的信息 中的producers字段
type NSQLookupProducer struct {
	RemoteAddress    string   `json:"remote_address"`
	HostName         string   `json:"hostname"`
	BroadcastAddress string   `json:"broadcast_address"`
	TCPPort          int      `json:"tcp_port"`
	HTTPort          int      `json:"http_port"`
	Version          string   `json:"version"`
	Tombstones       []bool   `json:"tombstones"`
	Topics           []string `json:"topics"`
}

// NSQLookupNodes nsqlookup /nodes接口获取到的信息
type NSQLookupNodes struct {
	Producers []NSQLookupProducer `json:"producers"`
}

// GetAllAvailableNSQNodesFromNSQLookup 从nslookup节点获取所有node节点
func GetAllAvailableNSQNodesFromNSQLookup(nsqlookupdAddr string) ([]NSQLookupProducer, error) {
	req, err := http.NewRequest(http.MethodGet, fmt.Sprintf("http://%s/nodes", nsqlookupdAddr), nil)
	if err != nil {
		return nil, err
	}

	response, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}

	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}

	nsqNodes := NSQLookupNodes{}
	if err := json.Unmarshal(body, &nsqNodes); err != nil {
		return nil, err
	}

	return nsqNodes.Producers, nil
}
