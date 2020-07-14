package nsqproducer

import (
	"strconv"
	"testing"
	"time"

	"github.com/nsqio/go-nsq"
)

func TestProducer(t *testing.T) {
	publisher, err := NewNSQPublisher([]string{"172.20.35.4:4161"}, 5*time.Second, nsq.NewConfig())
	if err != nil {
		t.Fatalf("初始化发布者失败:%+v", err)
	}

	topic := "TestProducer"
	for i := 0; i < 1000; i++ {
		if err := publisher.Publish(topic, []byte(strconv.Itoa(i))); err != nil {
			t.Fatalf("发布消息到Topic %s时发生错误:%+v", topic, err)
		}
	}

	t.Log("当前启动的nsqd节点:")
	for nsqAddr := range publisher.publishNodes {
		t.Log(nsqAddr)
	}
}
