from kafka import KafkaProducer
import csv
import logging

    # Set up logging
logging.basicConfig(filename='part1.log', level=logging.DEBUG)

# Log start of script
logging.info('Starting Part 1')

def read_csv(filename):
    with open(filename) as f:
        reader = csv.reader(f)
        next(reader) # skip header rowpip
        for row in reader:
            # Log successful file read
            logging.info(f'Read row from CSV: {row}')

            # Format the row as a comma-separated string
            message = ','.join(row)

            # Publish the message to Kafka
            producer.send('my-kafka-topic', message.encode('utf-8'))
            
            # Log successful message publish
            logging.info(f'Published message to Kafka: {message}')

if __name__ == '__main__':

    # Create a Kafka producer
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

    # Read the CSV file and publish each row to Kafka
    read_csv('pit_stops.csv')
