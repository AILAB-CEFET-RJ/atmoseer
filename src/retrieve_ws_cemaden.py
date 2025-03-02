import datetime as dt
import pandas as pd
import requests
import argparse
import time

# Constant for retry delay
RETRY_DELAY = 1800  # 30 minutes in seconds

def download_weather_data(uf, id_station, start_date, end_date, output_file, log_file):
    start_datetime = dt.datetime.strptime(start_date, "%Y%m%d%H%M")
    end_datetime = dt.datetime.strptime(end_date, "%Y%m%d%H%M")

    current_datetime = start_datetime
    dataframes = []
    log_data = []
    start_time = time.time()
    contador = 0

    while current_datetime <= end_datetime:
        try:
            formatted_date = current_datetime.strftime("%d/%m/%Y %H:%M")
            print(f"Downloading data for: {formatted_date}")
            
            url = f"http://sjc.salvar.cemaden.gov.br/resources/graficos/interativo/getJson2.php?uf={uf}&idestacao={id_station}&datahoraUltimovalor={formatted_date}"
            response = requests.get(url)
            response.raise_for_status()
            
            clima = response.json()

            if clima:
                df = pd.DataFrame(clima)
                dataframes.append(df)
                log_entry = create_log_entry(formatted_date, "Success")
                log_data.append(log_entry)
            else:
                log_entry = create_log_entry(formatted_date, "Error: No data")
                log_data.append(log_entry)
            contador += 1

        except requests.exceptions.RequestException as e:
            print(f"Request error: {e}")
            log_entry = create_log_entry(formatted_date, f"Request error: {e}")
            log_data.append(log_entry)
            time.sleep(RETRY_DELAY)  # Retry after delay

        current_datetime += dt.timedelta(hours=1)

    end_time = time.time()
    total_time = end_time - start_time
    
    final_df = pd.concat(dataframes, ignore_index=True)
    final_df.to_csv(output_file, index=False)

    log_df = pd.DataFrame(log_data)
    log_df.to_csv(log_file, index=False)

def create_log_entry(timestamp, status):
    """Helper function to create a log entry."""
    return {
        "datahoraUltimovalor": timestamp,
        "Status": status,
    }

def parse_arguments():
    """Parse command-line arguments."""
    description = "Choose the State, station, and the start and end dates."
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("-e", "--estado", required=True, help="State")
    parser.add_argument("-c", "--idestacao", required=True, help="Station ID")
    parser.add_argument("-start", "--start_date", required=True, help="Start date in format YYYYMMDDHHMM")
    parser.add_argument("-end", "--end_date", required=True, help="End date in format YYYYMMDDHHMM")
    parser.add_argument("-o", "--output_file", required=True, help="Output CSV file name")
    return parser.parse_args()


'''
CEMADEN (National Center for Monitoring and Early Warning of Natural Disasters) is a Brazilian agency responsible for monitoring natural hazards.
This programa downloads weather station data from the CEMADEN website, given a state, station ID, and a date range.
The data is saved in a CSV file and a log file is created to keep track of the download status.

# Example usage:
# python retrieve_ws_cemaden.py -e "RJ" -c "330455718A" -start "202303010600" -end "202303031200" -o "output_data.csv"
'''
if __name__ == "__main__":
    args = parse_arguments()
    log_file = "log.csv"
    download_weather_data(args.estado, args.idestacao, args.start_date, args.end_date, args.output_file, log_file)

