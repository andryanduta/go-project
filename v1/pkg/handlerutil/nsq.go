package handlerutil

import (
	"log"
	"os"
	"time"

	go_nsq "github.com/nsqio/go-nsq"
)

// NSQConfig denotes configuration for an nsq handler
type NSQConfig struct {
	Name               string
	LookupdAddress     string
	TopicName          string
	ChannelName        string
	MaxInFlight        int
	NumOfConsumers     int
	MaxAttempts        int
	RequeueInterval    time.Duration
	Timeout            time.Duration
	MaxBatchSize       int
	MaxBatchTime       time.Duration
	BatchWorker        int
	MaxBatchResyncTime time.Duration
}

// Consumer denotes setting for an NSQ Consumer
type Consumer struct {
	Config  NSQConfig
	Handler go_nsq.Handler
}

// ConsumerIdentity denotes nsq consumer identity
type ConsumerIdentity struct {
	ConfigName  string
	TopicName   string
	ChannelName string
}

// Start run nsq the consumer
func (c *Consumer) Start() error {
	nsqConfig := go_nsq.NewConfig()
	nsqConfig.MaxInFlight = c.Config.MaxInFlight
	nsqConfig.MaxAttempts = uint16(c.Config.MaxAttempts)
	nsqConfig.MsgTimeout = c.Config.Timeout
	nsqConfig.DefaultRequeueDelay = c.Config.RequeueInterval

	var q *go_nsq.Consumer

	q, err := go_nsq.NewConsumer(c.Config.TopicName, c.Config.ChannelName, nsqConfig)
	if err != nil {
		return err
	}

	q.AddConcurrentHandlers(c.Handler, c.Config.NumOfConsumers)
	q.SetLogger(log.New(os.Stderr, "nsq:", log.Ltime), go_nsq.LogLevelError)

	return q.ConnectToNSQLookupd(c.Config.LookupdAddress)
}
