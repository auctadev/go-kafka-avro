package kafka

import (
	"encoding/binary"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
	"github.com/linkedin/goavro"
	"os"
	"os/signal"
)

type AvroConsumer struct {
	Consumer             *cluster.Consumer
	SchemaRegistryClient *CachedSchemaRegistryClient
	callbacks            ConsumerCallbacks
}

type ConsumerCallbacks struct {
	OnDataReceived func(msg *Message) bool
	OnError        func(err error)
	OnCommitFailed func(msg *Message, err error)
	OnNotification func(notification *cluster.Notification)
}

type Message struct {
	SchemaId  int
	Topic     string
	Partition int32
	Offset    int64
	Key       string
	Value     string
}

const (
	COMMIT_ERROR_CTX = "commit"
)

// avroConsumer is a basic consumer to interact with schema registry, avro and kafka
func NewAvroConsumer(kafkaServers []string, schemaRegistryServers []string,
	topic string, groupId string, callbacks ConsumerCallbacks) (*AvroConsumer, error) {
	return NewAvroConsumerCustomConfig(kafkaServers, schemaRegistryServers, topic, groupId, callbacks, nil)
}

func NewAvroConsumerCustomConfig(kafkaServers []string, schemaRegistryServers []string,
	topic string, groupId string, callbacks ConsumerCallbacks, config *cluster.Config) (*AvroConsumer, error) {
	if config == nil {
		// init (custom) config, enable errors and notifications
		config = cluster.NewConfig()
		//read from beginning at the first time
		config.Consumer.Offsets.Initial = sarama.OffsetOldest
		config.Consumer.Return.Errors = true
		config.Group.Return.Notifications = true
	}

	topics := []string{topic}
	consumer, err := cluster.NewConsumer(kafkaServers, groupId, topics, config)
	if err != nil {
		return nil, err
	}

	schemaRegistryClient := NewCachedSchemaRegistryClient(schemaRegistryServers)
	return &AvroConsumer{
		consumer,
		schemaRegistryClient,
		callbacks,
	}, nil
}

//GetSchemaId get schema id from schema-registry service
func (ac *AvroConsumer) GetSchema(id int) (*goavro.Codec, error) {
	codec, err := ac.SchemaRegistryClient.GetSchema(id)
	if err != nil {
		return nil, err
	}
	return codec, nil
}

func (ac *AvroConsumer) Consume() error {
	// trap SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	// current managed message
	currentMsgChan := make(chan *Message, 1)

	// consume errors
	go func() {
		for err := range ac.Consumer.Errors() {
			if ac.callbacks.OnError != nil {
				if clusterErr, ok := err.(*cluster.Error); ok {
					switch clusterErr.Ctx {
					case COMMIT_ERROR_CTX:
						var msg *Message
						select {
						case msg = <-currentMsgChan:
						default:
							msg = nil
						}
						ac.callbacks.OnCommitFailed(msg, err)
					}
				}
				ac.callbacks.OnError(err)
			}
		}
	}()

	// consume notifications
	go func() {
		for notification := range ac.Consumer.Notifications() {
			if ac.callbacks.OnNotification != nil {
				ac.callbacks.OnNotification(notification)
			}
		}
	}()

	for {
		select {
		case m, ok := <-ac.Consumer.Messages():
			if ok {
				msg, err := ac.ProcessAvroMsg(m)
				if err != nil {
					ac.callbacks.OnError(err)
				} else {
					currentMsgChan = make(chan *Message, 1)
					currentMsgChan <- &msg
					markOffset := true
					if ac.callbacks.OnDataReceived != nil {
						if !ac.callbacks.OnDataReceived(&msg) {
							fmt.Printf("Failed to manage consumed message with offset [%+v] on partition [%d] for topic [%s]. Skipping offset commit", m.Offset, m.Partition, m.Topic)
							markOffset = false
						}
					}
					if markOffset {
						ac.Consumer.MarkOffset(m, "")
					}
				}
			}
		case <-signals:
			return nil
		}
	}

	return nil
}

func (ac *AvroConsumer) ProcessAvroMsg(m *sarama.ConsumerMessage) (Message, error) {
	schemaId := binary.BigEndian.Uint32(m.Value[1:5])
	codec, err := ac.GetSchema(int(schemaId))
	if err != nil {
		return Message{}, err
	}
	// Convert binary Avro data back to native Go form
	native, _, err := codec.NativeFromBinary(m.Value[5:])
	if err != nil {
		return Message{}, err
	}

	// Convert native Go form to textual Avro data
	textual, err := codec.TextualFromNative(nil, native)

	if err != nil {
		return Message{}, err
	}
	msg := Message{int(schemaId), m.Topic, m.Partition, m.Offset, string(m.Key), string(textual)}
	return msg, nil
}

func (ac *AvroConsumer) Close() {
	ac.Consumer.Close()
}
