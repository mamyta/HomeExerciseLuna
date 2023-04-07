
# HomeExerciseLuna

## Task 1

**Pitstop data will be ingested to the system via kafka**
Write a simple worker that puts the CSV data (line by line, from old to new) into the kafka stream

**ChatGPT**: 
Part 1 of the application is responsible for reading data from a CSV file and inserting it into a Kafka topic. Specifically, it reads a file called pit_stops.csv that contains data about pit stops made by drivers during Formula 1 races.

For each row in the CSV file, Part 1 constructs a message that contains several fields, including the race ID, driver ID, stop number, lap number, and duration of the pit stop. It then publishes this message to a Kafka topic using the Python Kafka library.

By publishing this data to a Kafka topic, Part 1 enables other parts of the application (such as Part 2) to consume and process this data in real-time. This is a common pattern in modern distributed systems, where data is decoupled from the application logic and processed asynchronously by different components.

**File** part1.py 

## Task 2
**RT calc of Average Duration of pit stop per driver name**
no need to stream driver names, you can offline store it anywhere you wish 
For this part you will need to “enrich” the data in RT, think of the ways you can do such.

 - Make sure you protect yourself from duplications. 
 - Make sure you protect yourself from corrupted data.

**ChatGPT**: 
Part 2 of the application is responsible for consuming data from a Kafka topic and calculating the average pit stop duration for each driver in real-time. Specifically, it consumes messages from the Kafka topic created by Part 1 and performs the following steps:

1.  Parse the message from Kafka and extract the relevant fields, including the race ID, driver ID, stop number, lap number, and duration of the pit stop.
2.  Check that the message contains all the required fields and that the duration is a valid float number.
3.  Construct a unique key based on the race ID, driver ID, and stop number. This key is used to prevent duplicates and ensure that only the most recent pit stop duration is used for each driver.
4.  Calculate the average pit stop duration for each driver based on the latest data available. Store these values in a dictionary.
5.  Log the average pit stop duration for each driver to the console.

By consuming data from Kafka and processing it in real-time, Part 2 allows the application to provide up-to-date information about pit stop performance for each driver. This information can be used by teams and fans to gain insights into the race and make informed decisions about strategy.

**File** part2.py 

## Daily offline report:
The data should be sinked into a [Data Store (of your choosing)]
Provide a daily report that for each race it will present:
Date (Link - can be stored offline)
The 1st, 2nd and 3rd positions driver's  names
Winning time

**ChatGPT**: 
Part 3 of the application reads data from a CSV file called results.csv, which contains information about the results of a Formula 1 race, including the race ID, driver ID, and other details. It then generates a report that summarizes the results of the race, including the circuit name, date, driver names for positions 1-3, and lap times.

To generate the report, Part 3 joins data from the results.csv file with information from the drivers.csv file to obtain the driver name for each driver ID, and with information from the circuits.csv file to obtain the circuit name for each circuit ID. The report is returned as a list of dictionaries, where each dictionary represents a row in the report.

Once the report is generated, Part 3 uses the Google Drive API to upload the report to Google Drive. This allows the report to be easily shared and accessed by other users.

**File** part3.py 
