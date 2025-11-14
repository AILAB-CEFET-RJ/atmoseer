import pandas as pd
import requests
from xml.etree import ElementTree as ET
from io import StringIO
import time
import sys
import argparse 
import os 

def fetch_websirene_data(timestamp_str: str, station_id_to_filter: int) -> pd.DataFrame | None:
    """
    Fetches and processes rain data from WebSirene for a specific timestamp,
    filtering for a single numeric station ID.
    """
    url_timestamp = timestamp_str.replace("'", "")
    url = f"http://websirene.rio.rj.gov.br/xml/chuvas.xml?time={url_timestamp}"
    
    print(f"Fetching data for: {timestamp_str} (URL: {url})") 
    
    xml_content = None 

    try:
        response = requests.get(url, timeout=30) 
        response.raise_for_status()  
        
        try:
            xml_content = response.content.decode('latin-1')
        except UnicodeDecodeError:
            print("latin-1 decoding failed, trying utf-8...")
            xml_content = response.content.decode('utf-8')

        xml_file = StringIO(xml_content)
       
        # Read all stations from the XML
        df = pd.read_xml(xml_file, xpath=".//estacao") 
        
        # Column that identifies the numeric ID (should be 'id' from <estacao id="...">)
        station_id_column = 'id' 

        if station_id_column not in df.columns:
            print(f"-> Warning: Numeric ID column '{station_id_column}' not found in XML for {timestamp_str}. Skipping.")
            return None

        # Ensure the 'id' column in the DataFrame is numeric for comparison
        try:
            df[station_id_column] = pd.to_numeric(df[station_id_column])
        except ValueError:
            print(f"-> Warning: Could not convert column '{station_id_column}' to numeric in {timestamp_str}. Skipping.")
            return None

        # Filter the DataFrame to contain ONLY the station with the desired numeric ID
        df_filtered = df[df[station_id_column] == station_id_to_filter].copy() # Use .copy()

        if df_filtered.empty:
            print(f"-> Info: Station with ID '{station_id_to_filter}' not found (or no data) on {timestamp_str}.")
            return None

        # Rename the 'chuvas' column (from XML) to 'rains' (chuvas is the translation to rains)
        if "chuvas" in df_filtered.columns:
            df_filtered = df_filtered.rename(columns={"chuvas": "rains"})
        else:
            print(f"-> Warning: 'chuvas' column not found for ID {station_id_to_filter} at {timestamp_str}. 'rains' column will be missing.")
        
        # Add datetime AFTER filtering and renaming
        df_filtered['datetime'] = pd.to_datetime(timestamp_str, format='%d/%m/%Y %H:%M')
        
        print(f"Data found for ID '{station_id_to_filter}' on {timestamp_str}.") 
        return df_filtered # Return the filtered and renamed DataFrame

    except requests.exceptions.HTTPError as e:
        print(f"-> Warning: Failed to fetch data for {timestamp_str}. Error: {e}")
        if response.status_code == 404:
            print("Error 404: Possibly no data for this timestamp.")
        return None
    except requests.exceptions.RequestException as e:
        print(f"-> Warning: Connection/Request error accessing {url}: {e}")
        return None
    except pd.errors.ParserError as e:
        print(f"-> Warning: Pandas error reading XML for {timestamp_str}: {e}")
        if xml_content: print(f"XML content (start): {xml_content[:500]}")
        return None
    except ET.ParseError as e: 
        print(f"-> Warning: Error parsing XML for {timestamp_str}: {e}")
        if xml_content: print(f"XML content (start): {xml_content[:500]}")
        return None
    except ValueError as e: 
        print(f"-> Warning: Error processing XML for {timestamp_str}. Possible empty XML or no '<estacao>' tag. Error: {e}")
        if xml_content: print(f"XML content (start): {xml_content[:500]}")
        return None
    except Exception as e:
        print(f"-> Warning: Unexpected error processing {timestamp_str}. Error: {e}")
        return None


def main(start_date_arg, end_date_arg, output_dir_arg, station_id_arg: int):
    """
    Main function that performs data collection for one specific station (by numeric ID).
    """
    print(f"--- Starting WebSirene data collection pipeline ---")
    print(f"Target Station (Numeric ID): {station_id_arg}") 
    print(f"Period: {start_date_arg} to {end_date_arg}")
    
    
    try:
        # Generate timestamps
        timestamps = pd.date_range(start=start_date_arg, end=end_date_arg, freq='10min') 
    except Exception as e:
        print(f"Error generating date range: {e}. Check formats.")
        sys.exit(1) 
        
    all_dataframes = []
    total_requests = len(timestamps)
    
    
    for i, ts in enumerate(timestamps):
        timestamp_str = ts.strftime('%d/%m/%Y %H:%M')
        print(f"Processing {i+1}/{total_requests}: {timestamp_str}")
        
        # Fetch data, already filtered by station
        df_chuva = fetch_websirene_data(timestamp_str, station_id_arg) 
        
        if df_chuva is not None and not df_chuva.empty:
            # Add the dataframe (only for the target station) to the list
            all_dataframes.append(df_chuva)
        

    if not all_dataframes:
        print(f"\nNo data was collected for station ID '{station_id_arg}' in the specified period.")
        return

    
    print("\nConcatenating all collected data...")
    # Concatenate all dataframes into one (now only contains the target station)
    final_df = pd.concat(all_dataframes, ignore_index=True)
    

    # 1. Create the output directory if it doesn't exist
    os.makedirs(output_dir_arg, exist_ok=True)
    print(f"Output directory set to: '{output_dir_arg}'")

    # 2. Create a safe filename based on the station ID
    #    The filename will be '22.csv', '1.csv', etc.
    safe_station_id = str(station_id_arg) 
    output_path = os.path.join(output_dir_arg, f"{safe_station_id}.csv")
    
    print(f"Saving filtered data for ID '{station_id_arg}' ({len(final_df)} records) to {output_path}...")
    
    # 3. Save the CSV for this station, sorted by date
    station_df_sorted = final_df.sort_values(by='datetime')
    station_df_sorted.to_csv(output_path, index=False, encoding='utf-8')
    
    print(f"\n--- Collection pipeline (for specific ID) finished! ---")
    print(f"Total records saved: {len(station_df_sorted)}")
    

if __name__ == "__main__":
    
    parser = argparse.ArgumentParser(description="WebSirene rain data collector for ONE specific station (by numeric ID).")
    
    
    parser.add_argument('--start-date', required=True, 
                        help="Start date and time (format: 'DD-MM-YYYY HH:MM')")
                        
    
    parser.add_argument('--end-date', required=True, 
                        help="End date and time (format: 'DD-MM-YYYY HH:MM')")
                        
    
    parser.add_argument('--station-id', required=True, type=int,
                        help="Numeric ID of the specific station to collect (e.g., 1, 22, 34)")
    
    parser.add_argument('--output-dir', default='data/ws/websirene', 
                        help="Output directory for the station's file (default: data/ws/websirene)")
    
    args = parser.parse_args()
    
   
    main(args.start_date, args.end_date, args.output_dir, args.station_id)
   

