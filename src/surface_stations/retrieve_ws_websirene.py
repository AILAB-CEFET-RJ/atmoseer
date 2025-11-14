import argparse
import os
import sys
import time
from datetime import datetime
from xml.etree import ElementTree as ET

import pandas as pd
import requests

REQUEST_INTERVAL_SECONDS = 0.2
MAX_FETCH_ATTEMPTS = 3
BACKOFF_SECONDS = 5


def _clean_text(value: str | None) -> str:
    if value is None:
        return ""
    return value.strip()


def _parse_numeric(value: str | None) -> float | None:
    if value is None:
        return None
    cleaned = value.strip()
    if not cleaned or cleaned.lower() == "null":
        return None
    cleaned = cleaned.replace(",", ".")
    try:
        return float(cleaned)
    except ValueError:
        return None


def _extract_rain_value(station_elem: ET.Element) -> float | None:
    """
    Return the m5 attribute inside <chuvas> which represents rainfall for the last 5 minutes.
    """
    chuvas_elem = station_elem.find("chuvas")
    if chuvas_elem is None:
        return None

    if chuvas_elem.get("m5") is None:
        # fall back to legacy behaviour if m5 isn't present
        fallback_sources = [
            station_elem.get("chuvas"),
            station_elem.get("chuva"),
            station_elem.findtext("chuvas"),
        ]
        for candidate in chuvas_elem.findall(".//chuva"):
            fallback_sources.append(_clean_text(candidate.text))
        for source in fallback_sources:
            parsed = _parse_numeric(source)
            if parsed is not None:
                return parsed
        return None

    return _parse_numeric(chuvas_elem.get("m5"))


def _extract_coordinates(station_elem: ET.Element) -> tuple[float | None, float | None]:
    loc_elem = station_elem.find("localizacao")
    if loc_elem is None:
        return None, None
    latitude = _parse_numeric(loc_elem.get("latitude"))
    longitude = _parse_numeric(loc_elem.get("longitude"))
    return latitude, longitude


def _extract_station_payload(xml_content: str, station_id_to_filter: int) -> dict | None:
    root = ET.fromstring(xml_content)
    for station_elem in root.findall(".//estacao"):
        raw_id = station_elem.get("id")
        if raw_id is None:
            continue
        try:
            current_id = int(raw_id)
        except ValueError:
            continue
        if current_id != station_id_to_filter:
            continue

        latitude, longitude = _extract_coordinates(station_elem)

        payload = {
            "id": current_id,
            "nome": _clean_text(station_elem.get("nome")),
            "type": _clean_text(station_elem.get("type")),
            "latitude": latitude,
            "longitude": longitude,
            "chuvas": _extract_rain_value(station_elem),
        }
        return payload
    return None


def fetch_websirene_data(timestamp_str: str, station_id_to_filter: int) -> pd.DataFrame | None:
    """
    Fetches and processes rain data from WebSirene for a specific timestamp,
    filtering for a single numeric station ID.
    """
    url_timestamp = timestamp_str.replace("'", "")
    url = f"http://websirene.rio.rj.gov.br/xml/chuvas.xml?time={url_timestamp}"

    print(f"Fetching data for: {timestamp_str} (URL: {url})")
    xml_content: str | None = None
    for attempt in range(1, MAX_FETCH_ATTEMPTS + 1):
        response = None
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()

            encoding = response.encoding or response.apparent_encoding or "latin-1"
            response.encoding = encoding
            xml_content = response.text

            payload = _extract_station_payload(xml_content, station_id_to_filter)
            if payload is None:
                print(f"-> Info: Station with ID '{station_id_to_filter}' not found (or no data) on {timestamp_str}.")
                return None

            df_filtered = pd.DataFrame([payload])
            if "chuvas" in df_filtered.columns:
                df_filtered = df_filtered.rename(columns={"chuvas": "rains"})

            df_filtered["datetime"] = pd.to_datetime(timestamp_str, format="%d/%m/%Y %H:%M")
            print(f"Data found for ID '{station_id_to_filter}' on {timestamp_str}.")
            return df_filtered

        except requests.exceptions.HTTPError as e:
            print(f"-> Warning: Failed to fetch data for {timestamp_str}. Error: {e}")
            if response is not None and response.status_code == 404:
                print("Error 404: Possibly no data for this timestamp.")
        except requests.exceptions.RequestException as e:
            print(f"-> Warning: Connection/Request error accessing {url}: {e}")
        except ET.ParseError as e:
            sample = (xml_content[:500] + "...") if xml_content else "No XML content."
            print(f"-> Warning: XML parsing error for {timestamp_str}: {e}. Sample: {sample}")
        except ValueError as e:
            sample = (xml_content[:500] + "...") if xml_content else "No XML content."
            print(f"-> Warning: Error processing XML for {timestamp_str}: {e}. Sample: {sample}")
        except Exception as e:
            print(f"-> Warning: Unexpected error processing {timestamp_str}. Error: {e}")

        if attempt < MAX_FETCH_ATTEMPTS:
            sleep_time = BACKOFF_SECONDS * attempt
            print(f"-> Info: Retrying {timestamp_str} in {sleep_time} seconds (attempt {attempt + 1}/{MAX_FETCH_ATTEMPTS})...")
            time.sleep(sleep_time)
    return None


def parse_cli_datetime(raw_datetime: str) -> datetime:
    """Parse expected CLI datetime formats and raise ValueError if none match."""
    supported_formats = ("%d/%m/%Y %H:%M", "%d-%m-%Y %H:%M")
    for fmt in supported_formats:
        try:
            return datetime.strptime(raw_datetime, fmt)
        except ValueError:
            continue
    raise ValueError(f"'{raw_datetime}' is not in a supported format (expected DD/MM/YYYY HH:MM or DD-MM-YYYY HH:MM).")


def main(start_date_arg, end_date_arg, output_dir_arg, station_id_arg: int):
    """
    Main function that performs data collection for one specific station (by numeric ID).
    """
    print(f"--- Starting WebSirene data collection pipeline ---")
    print(f"Target Station (Numeric ID): {station_id_arg}")

    try:
        start_dt = parse_cli_datetime(start_date_arg)
        end_dt = parse_cli_datetime(end_date_arg)
    except ValueError as err:
        print(f"Invalid datetime: {err}")
        sys.exit(1)

    if end_dt < start_dt:
        print("End date/time must be greater than or equal to the start date/time.")
        sys.exit(1)

    print(f"Period: {start_dt.strftime('%d/%m/%Y %H:%M')} to {end_dt.strftime('%d/%m/%Y %H:%M')}")

    try:
        # Generate timestamps
        timestamps = pd.date_range(start=start_dt, end=end_dt, freq='10min')
    except Exception as e:
        print(f"Error generating date range: {e}. Check formats.")
        sys.exit(1) 

    if timestamps.empty:
        print("No timestamps were generated. Please verify the provided dates.")
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
        time.sleep(REQUEST_INTERVAL_SECONDS)

    if not all_dataframes:
        print(f"\nNo data was collected for station ID '{station_id_arg}' in the specified period.")
        sys.exit(1)

    
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
                        help="Start date and time (formats: 'DD/MM/YYYY HH:MM' or 'DD-MM-YYYY HH:MM')")
                        
    
    parser.add_argument('--end-date', required=True, 
                        help="End date and time (formats: 'DD/MM/YYYY HH:MM' or 'DD-MM-YYYY HH:MM')")
                        
    
    parser.add_argument('--station-id', required=True, type=int,
                        help="Numeric ID of the specific station to collect (e.g., 1, 22, 34)")
    
    parser.add_argument('--output-dir', default='data/ws/websirene', 
                        help="Output directory for the station's file (default: data/ws/websirene)")
    
    args = parser.parse_args()
    
   
    main(args.start_date, args.end_date, args.output_dir, args.station_id)
   
