package nsq

import (
	"encoding/json"
	"errors"
	"log"
	"time"

	"github.com/nsqio/go-nsq"
	gcfg "gopkg.in/gcfg.v1"
)

type (
	// Define struct for nsq config
	NsqConfig struct {
		Connection map[string]*struct {
			Producer string
		}
	}

	// NSQ producers
	NsqProducerList struct {
		Name map[string]NsqProducer
	}
)

//go:generate mockgen -destination ./nsq_mock.go -package nsq github.com/adg/go-project/src/util/database/nsq NsqProducer

// NsqProducer is a simple interface to nsq.Producer so we can mock it
// for unit testing.
type NsqProducer interface {
	Publish(topic string, body []byte) error
	MultiPublish(topic string, bodies [][]byte) error
	PublishAsync(topic string, body []byte, doneChan chan *nsq.ProducerTransaction, args ...interface{}) error
	DeferredPublish(topic string, delay time.Duration, body []byte) error
}

var (
	NsqCfg            *NsqConfig
	nsqProducer       *nsq.Producer
	NsqDBProducerList = &NsqProducerList{Name: make(map[string]NsqProducer)}

	ErrNoConnection = errors.New("No connection found")
)

func NewNsqConfig(filePath string) (*NsqConfig, error) {
	if NsqCfg == nil {
		var c NsqConfig

		err := gcfg.ReadFileInto(&c, filePath)
		if err != nil {
			return nil, errors.New("Could not load config file: " + filePath)
		}

		NsqCfg = &c
	}

	return NsqCfg, nil
}

func NewNsqProducerAll() error {
	if NsqCfg == nil {
		return errors.New("Config nsq variable not initialized")
	}

	for name := range NsqCfg.Connection {
		NewNsqProducerV2(name)
		if _, exists := NsqDBProducerList.Name[name]; !exists {
			log.Fatalln("No nsq connection has been created on: " + name)
		}
	}

	return nil
}

func NewNsqProducerV2(clientName string) NsqProducer {
	var err error
	conn := NsqCfg.Connection[clientName].Producer

	config := nsq.NewConfig()
	nsqProducer, err = nsq.NewProducer(conn, config)
	if err != nil {
		log.Println(err)
	}

	NsqDBProducerList.Name[clientName] = nsqProducer
	return NsqDBProducerList.Name[clientName]
}

func GetProducer(clientName string) (NsqProducer, error) {
	cli, exists := NsqDBProducerList.Name[clientName]
	if exists {
		return cli, nil
	}

	return nil, ErrNoConnection
}

func GetConnectionName(clientName string) string {
	return NsqCfg.Connection[clientName].Producer
}

// Publish publishes data to specific NSQ topic
func Publish(topicName string, data interface{}) error {

	msg, err := json.Marshal(data)
	if err != nil {
		return err
	}

	nsqProducer, err := GetProducer("local")
	if err != nil {
		return err
	}

	err = nsqProducer.Publish(topicName, msg)
	if err != nil {
		return err
	}

	return nil
}

// DeferredPublish publishes data to specific NSQ topic with delay time
func DeferredPublish(topicName string, delay time.Duration, data interface{}) error {

	msg, err := json.Marshal(data)
	if err != nil {
		return err
	}

	nsqProducer, err := GetProducer("local")
	if err != nil {
		return err
	}

	err = nsqProducer.DeferredPublish(topicName, delay, msg)
	if err != nil {
		return err
	}

	return nil
}
