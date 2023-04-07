import os
import csv
import json
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import googleapiclient
import logging

# Set up logging
logging.basicConfig(filename='part3.log', level=logging.DEBUG)

# Log start of script
logging.info('Starting Part 3')

def upload_file_to_google_drive(file_path, creds_file_path):
    # Load the Google Drive API credentials from the JSON file
    creds = None
    if os.path.exists(creds_file_path):
        with open(creds_file_path, 'r') as creds_file:
            creds_info = json.load(creds_file)
            creds = Credentials.from_authorized_user_info(info=creds_info)

    if not creds:
        print("Could not authenticate with Google Drive API. Please check your credentials.")
        return

    # Build the Google Drive API client
    service = build('drive', 'v3', credentials=creds)

    # Set the metadata for the file
    file_metadata = {'name': os.path.basename(file_path)}

    # Upload the file to Google Drive
    try:
        media = googleapiclient.http.MediaFileUpload(file_path, resumable=True)
        file = service.files().create(body=file_metadata, media_body=media, fields='webViewLink').execute()
        logging.info(f"File URL: {file.get('webViewLink')}")
    except HttpError as error:
        logging.warning(f"An error occurred: {error}")
        file = None

    return file.get('webViewLink') if file else None




def build_report():
    # Load the drivers.csv file into a dictionary for efficient lookup
    drivers = {}
    with open('drivers.csv') as f:
        reader = csv.reader(f)
        next(reader) # skip header row
        for row in reader:
            driver_id = row[0]
            driver_name = f"{row[4]} {row[5]}"
            drivers[driver_id] = driver_name

    # Load the circuits.csv file into a dictionary for efficient lookup
    circuits = {}
    with open('circuits.csv') as f:
        reader = csv.reader(f)
        next(reader) # skip header row
        for row in reader:
            circuit_id = row[0]
            circuit_name = row[2]
            circuits[circuit_id] = circuit_name

    # Load the races.csv file into a dictionary for efficient lookup
    race_info = {}
    with open('races.csv') as f:
        reader = csv.reader(f)
        next(reader) # skip header row
        for row in reader:
            race_id, year, round_num, circuit_id, name, date, time, url, fp1_date, fp1_time, fp2_date, fp2_time, fp3_date, fp3_time, quali_date, quali_time, sprint_date, sprint_time = row
            race_info[race_id] = {'date': date, 'circuitId': circuit_id}

    # Load the results.csv file into a dictionary for efficient lookup
    race_results = {}
    with open('results.csv') as f:
        reader = csv.reader(f)
        next(reader) # skip header row
        for row in reader:
            result_id, race_id, driver_id, constructor_id, number, grid, position, position_text, position_order, points, laps, time, milliseconds, fastest_lap, rank, fastest_lap_time, fastest_lap_speed, status_id = row
            if position == '1':
                driver_name = drivers.get(driver_id, '')
                race_results.setdefault(race_id, {})['position1'] = driver_name
                race_results.setdefault(race_id, {})['time'] = time
            elif position == '2':
                driver_name = drivers.get(driver_id, '')
                race_results.setdefault(race_id, {})['position2'] = driver_name
            elif position == '3':
                driver_name = drivers.get(driver_id, '')
                race_results.setdefault(race_id, {})['position3'] = driver_name
            

    # Build the report based on the data
    report = [['Race ID', 'Race Date', 'Circuit Name', 'Position 1 Driver Name', 'Position 2 Driver Name', 'Position 3 Driver Name', 'Time']]
    for race_id, data in race_results.items():
        position1 = data.get('position1', '')
        position2 = data.get('position2', '')
        position3 = data.get('position3', '')
        time = data.get('time', '')
        race_date = race_info.get(race_id, {}).get('date', '')
        circuit_id = race_info.get(race_id, {}).get('circuitId', '')
        circuit_name = circuits.get(circuit_id, '')
        report.append([race_id, race_date, circuit_name, position1, position2, position3, time])

    # Return the report
    return report

def save_report_to_csv(file_path):
    # Build the report
    report = build_report()

    # Save the report to a CSV file
    with open(file_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerows(report)
    
    # Log report saved to file
    logging.info('Report saved to file')
    

if __name__ == '__main__':
    # Save the report to a CSV file
    report_file_path = 'report.csv'
    save_report_to_csv(report_file_path)

    # Upload the report to Google Drive
    url = upload_file_to_google_drive(report_file_path,'token.json')

    # Log report uploaded to Google Drive
    logging.info(f'Report uploaded to Google Drive: {url}')