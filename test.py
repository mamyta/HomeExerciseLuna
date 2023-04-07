import unittest
from kafka import KafkaConsumer, KafkaProducer

class TestKafkaProducer(unittest.TestCase):
    def test_produce_message(self):
        producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
        message = "1,1,1,100,1000"
        producer.send('my-kafka-topic', value=message.encode('utf-8'))
        producer.flush()
        producer.close()

        consumer = KafkaConsumer('my-kafka-topic', bootstrap_servers=['localhost:9092'], auto_offset_reset='earliest')
        messages = [message.value.decode('utf-8') for message in consumer]
        consumer.close()
        print(message)
        self.assertEqual(len(messages), 1)
        self.assertEqual(messages[0], message)

if __name__ == '__main__':
    unittest.main()