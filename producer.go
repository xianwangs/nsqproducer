package nsqproducer

import (
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nsqio/go-nsq"
)

type publishNode struct {
	// 当前节点是否正常
	avaliable bool
	// 修改 avaliable 需要的锁
	avaliableLock sync.RWMutex
	// nsq producer实例
	producer *nsq.Producer
}

func (p *publishNode) setAvaliable(avaliable bool) {
	p.avaliableLock.Lock()
	defer p.avaliableLock.Unlock()

	p.avaliable = avaliable
}

func (p *publishNode) getAvaliable() bool {
	p.avaliableLock.RLock()
	defer p.avaliableLock.RUnlock()

	return p.avaliable
}

// NSQPublisher nsq发布者
type NSQPublisher struct {
	// nsqlookup 的地址
	nsqlookupdAddrs []string
	// 通过查询nsqlookup /nodes接口会得到producer, publishNodes是对应到每一个producer的链接
	publishNodes map[string]*publishNode
	// publishNodes 的key的集合, 用于保证请求在各nsqd之间平均分配
	nsqdAddrs []string
	// 使用该递增值保证请求在各nsqd之间均匀分配
	publishNodeSelectIndex uint64
	// nsq producer的配置
	nsqCfg *nsq.Config
	// 每隔多久检查一次nsqlookup /nodes 以更新producer信息
	checkInterval time.Duration
	// 检查 nsqlookup /nodes 时需要获取的锁
	checkLock sync.RWMutex
}

func (n *NSQPublisher) nextPublishNode() (*publishNode, error) {
	if n.publishNodes == nil || len(n.publishNodes) == 0 {
		return nil, errors.New("nsq节点数为0")
	}

	// n.publishNodes 每次遍历出key的顺序无法保证, 所以使用n.nsqdAddrs预先处理好

	index := atomic.AddUint64(&n.publishNodeSelectIndex, 1)
	for i := 0; i < len(n.nsqdAddrs); i++ {
		if n.publishNodes[n.nsqdAddrs[index%uint64(len(n.publishNodes))]].getAvaliable() {
			return n.publishNodes[n.nsqdAddrs[index%uint64(len(n.publishNodes))]], nil
		}

		index++
	}

	return nil, errors.New("没有找到可用的节点")
}

// Publish 发布消息
func (n *NSQPublisher) Publish(topic string, body []byte) error {
	n.checkLock.RLock()
	defer n.checkLock.RUnlock()

	for {
		publishNode, err := n.nextPublishNode()
		// 没有节点可用
		if err != nil {
			return err
		}

		if err := publishNode.producer.Publish(topic, body); err != nil {
			publishNode.setAvaliable(false)
			continue
		}

		break
	}

	return nil
}

func (n *NSQPublisher) getNodeAddrs() []string {
	nodeAddrs := []string{}
	// 获取所有有效的nsq节点
	for _, nsqlookupdAddr := range n.nsqlookupdAddrs {
		producers, err := GetAllAvailableNSQNodesFromNSQLookup(nsqlookupdAddr)
		if err != nil {
			return nil
		}

		for _, producer := range producers {
			exist := false
			addr := producer.BroadcastAddress + ":" + strconv.Itoa(producer.TCPPort)
			for _, nodeAddr := range nodeAddrs {
				if nodeAddr == addr {
					exist = true
					break
				}
			}

			if !exist {
				nodeAddrs = append(nodeAddrs, addr)
			}
		}
	}

	return nodeAddrs
}

// 初始化nsq 链接, 并检查已有的链接的变化
func (n *NSQPublisher) initPool() {
	// 查询所有的nsqd节点列表
	nodeAddrs := n.getNodeAddrs()

	n.checkLock.Lock()
	defer n.checkLock.Unlock()

	if n.publishNodes == nil {
		n.publishNodes = map[string]*publishNode{}
	}

	// 从nsqlookup查询到的nsqd节点列表
	// 为新加入的nsqd节点建立新的连接
	// 为失败的nsqd节点尝试重建连接
	for _, nodeAddr := range nodeAddrs {
		// 有效的nsqd节点
		if v, ok := n.publishNodes[nodeAddr]; ok && v.avaliable {
			continue
		}

		producer, err := nsq.NewProducer(nodeAddr, n.nsqCfg)
		if err != nil {
			delete(n.publishNodes, nodeAddr)
			continue
		}

		n.publishNodes[nodeAddr] = &publishNode{
			avaliable: true,
			producer:  producer,
		}
	}

	n.nsqdAddrs = []string{}
	// 剔除下线的nsqd节点
	for addr := range n.publishNodes {
		exist := false
		// 本次从nsqlookup查询到的nsqd节点
		for _, nodeAddr := range nodeAddrs {
			if addr == nodeAddr {
				exist = true
				break
			}
		}

		if !exist {
			delete(n.publishNodes, addr)
			continue
		}

		n.nsqdAddrs = append(n.nsqdAddrs, addr)
	}
}

// 定时检查节点数量变化和无效节点
func (n *NSQPublisher) loop() {
	go func() {
		for {
			select {
			case <-time.NewTimer(n.checkInterval).C:
				n.initPool()
			}
		}
	}()
}

// 定时检查节点数的变化

// NewNSQPublisher 构建一个nsq发布者
// 参数 nsqlookupdAddrs 是nsqlookup的地址的集合
// 参数 checkInterval 表示每隔多久检查一次nsqlookup下nsqd节点数量的变化
// 参数 nsqCfg 是 nsq producer的配置, 如果传nil则默认使用nsq.NewConfig()配置对象
func NewNSQPublisher(nsqlookupdAddrs []string, checkInterval time.Duration, nsqCfg *nsq.Config) (*NSQPublisher, error) {
	if nsqCfg == nil {
		nsqCfg = nsq.NewConfig()
	}

	publisher := &NSQPublisher{
		nsqlookupdAddrs: nsqlookupdAddrs,
		checkInterval:   checkInterval,
		nsqCfg:          nsqCfg,
	}
	publisher.initPool()
	publisher.loop()

	return publisher, nil
}
