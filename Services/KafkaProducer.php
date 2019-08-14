<?php declare(strict_types=1);

namespace Infrastructure\Services;

class KafkaProducer
{
    /**
     * @var string
     */
    private $kafkaHost;

    /**
     * @var integer
     */
    private $kafkaPort;

    /**
     * @var array
     */
    private $topicInstances = [];

    /**
     * @var \RdKafka\Producer
     */
    private $producer;

    /**
     * KafkaProducer constructor.
     * @param string $kafkaHost
     * @param int $kafkaPort
     */
    public function __construct(string $kafkaHost, int $kafkaPort)
    {
        $this->kafkaHost = $kafkaHost;
        $this->kafkaPort = $kafkaPort;
    }

    public function sendMessage(string $topic, array $message): void
    {
        $this->topicInstance($topic)->produce(RD_KAFKA_PARTITION_UA, 0, json_encode($message));
    }

    private function topicInstance(string $topicName): \RdKafka\ProducerTopic
    {
        if (array_key_exists($topicName, $this->topicInstances)){
            return $this->topicInstances[$topicName];
        }

        $this->topicInstances[$topicName] = $this->producer()->newTopic($topicName);

        return $this->topicInstances[$topicName];
    }

    private function producer(): \RdKafka\Producer
    {
        if ($this->producer !== null){
            return $this->producer;
        }

        $this->producer = new \RdKafka\Producer();
        $this->producer->addBrokers($this->kafkaHost . ':' . $this->kafkaPort);

        return $this->producer;
    }
}