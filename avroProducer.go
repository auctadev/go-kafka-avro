package kafka

import (
	"encoding/binary"
	"github.com/Shopify/sarama"
	"github.com/linkedin/goavro"
)

type AvroProducer struct {
	producer             sarama.SyncProducer
	schemaRegistryClient *CachedSchemaRegistryClient
}

// NewAvroProducer is a basic producer to interact with schema registry, avro and kafka
func NewAvroProducer(kafkaServers []string, schemaRegistryServers []string) (*AvroProducer, error) {
	return NewAvroProducerCustomConfig(kafkaServers, schemaRegistryServers, nil)
}

func NewAvroProducerCustomConfig(kafkaServers []string, schemaRegistryServers []string, config *sarama.Config) (*AvroProducer, error) {
	if config == nil {
		config = sarama.NewConfig()
		config.Producer.Partitioner = sarama.NewRandomPartitioner
		config.Producer.Return.Successes = true
		config.Producer.RequiredAcks = sarama.WaitForAll
	}
	producer, err := sarama.NewSyncProducer(kafkaServers, config)
	if err != nil {
		return nil, err
	}
	schemaRegistryClient := NewCachedSchemaRegistryClient(schemaRegistryServers)
	return &AvroProducer{producer, schemaRegistryClient}, nil
}

//GetSchemaId get schema id from schema-registry service
func (ap *AvroProducer) GetSchemaId(topic string, avroCodec *goavro.Codec) (int, error) {
	schemaId, err := ap.schemaRegistryClient.CreateSubject(topic, avroCodec)
	if err != nil {
		return 0, err
	}
	return schemaId, nil
}

func (ap *AvroProducer) Add(topic string, schema string, key []byte, value []byte) error {
	_, _, err := ap.AddWithResponse(topic, schema, key, value)
	return err
}

func (ap *AvroProducer) AddWithResponse(topic string, schema string, key []byte, value []byte) (int32, int64, error) {
	avroCodec, err := goavro.NewCodec(schema)
	schemaId, err := ap.GetSchemaId(topic, avroCodec)
	if err != nil {
		return -1, -1, err
	}
	binarySchemaId := make([]byte, 4)
	binary.BigEndian.PutUint32(binarySchemaId, uint32(schemaId))

	native, _, err := avroCodec.NativeFromTextual(value)
	if err != nil {
		return -1, -1, err
	}

	// Convert native Go form to binary Avro data
	binaryValue, err := avroCodec.BinaryFromNative(nil, native)
	if err != nil {
		return -1, -1, err
	}

	var binaryMsg []byte
	// first byte is magic byte, always 0 for now
	binaryMsg = append(binaryMsg, byte(0))
	//4-byte schema ID as returned by the Schema Registry
	binaryMsg = append(binaryMsg, binarySchemaId...)
	//avro serialized data in Avro’s binary encoding
	binaryMsg = append(binaryMsg, binaryValue...)

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(key),
		Value: sarama.StringEncoder(binaryMsg),
	}
	return ap.producer.SendMessage(msg)
}

func (ac *AvroProducer) Close() {
	ac.producer.Close()
}
