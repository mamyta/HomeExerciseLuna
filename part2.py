from kafka import KafkaConsumer
import csv
import logging

# Set up logging
logging.basicConfig(filename='part2.log', level=logging.DEBUG)

# Log start of script
logging.info('Starting Part 2')

def consume_messages():
    # Load the drivers.csv file into a dictionary for efficient lookup
    drivers = {}
    with open('drivers.csv') as f:
        reader = csv.reader(f)
        next(reader) # skip header row
        for row in reader:
            driver_id = row[0]
            driver_name = f"{row[4]} {row[5]}"
            drivers[driver_id] = driver_name

    # Create a Kafka consumer
    consumer = KafkaConsumer('my-kafka-topic', bootstrap_servers=['localhost:9092'])

    # Keep track of the total duration and count for each driver_id and stop
    driver_stop_totals = {}
    driver_stop_counts = {}

    for message in consumer:
        # Log successful message consume
        logging.info(f'Consumed message from Kafka: {message.value.decode("utf-8")}')
        # Decode the message value and extract the fields
        fields = message.value.decode('utf-8').split(',')
        race_id, driver_id, stop, lap, time, duration, milliseconds = fields

        # Create a unique key for the driver_id and stop
        key = f"{race_id}_{driver_id}_{stop}"

        # Check if the key already exists to prevent duplicates
        if key in driver_stop_counts:
            # Log duplicate row
            logging.warning(f'Duplicate row: {row}')
            continue

        # Validate the data to prevent corrupted data
        if not race_id or not driver_id or not stop or not lap or not time or not duration or not milliseconds:
            logging.warning(f"Invalid data: {message.value}")
            continue

        # Validate the duration field
        try:
            duration = float(duration)
        except ValueError:
            logging.warning(f"Invalid duration: {duration} in row: {message.value}")
            continue

        # Calculate the total duration and count for the driver_id and stop
        if key not in driver_stop_totals:
            driver_stop_totals[key] = 0
            driver_stop_counts[key] = 0

        driver_stop_totals[key] += duration
        driver_stop_counts[key] += 1

        # Calculate and print the average duration for the driver_id and stop
        driver_name = drivers.get(driver_id, 'Unknown')

        driver_stop_avg_duration = driver_stop_totals[key] / driver_stop_counts[key]
        print(f"Driver {driver_name} (ID: {driver_id}), Stop {stop}, average duration: {driver_stop_avg_duration:.2f}")

if __name__ == '__main__':
    # Consume messages from Kafka and calculate average durations
    consume_messages()
