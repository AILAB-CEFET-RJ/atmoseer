import pandas as pd
from metpy.units import units
import metpy.calc as mpcalc
import argparse

def compute_indices(df_launch):
    df_launch_cleaned = df_launch.drop_duplicates(subset='pressure', ignore_index=True)

    df_launch_cleaned = df_launch_cleaned.dropna(subset=('temperature', 'dewpoint', 'pressure'), how='any').reset_index(drop=True)

    df_launch_cleaned = df_launch_cleaned.sort_values('pressure', ascending=False)
        
    pressure_values = df_launch_cleaned['pressure'].to_list() * units.hPa
    temperature_values = df_launch_cleaned['temperature'].to_list() * units.degC
    dewpoint_values = df_launch_cleaned['dewpoint'].to_list() * units.degC

    parcel_profile = mpcalc.parcel_profile(pressure_values, 
                                           temperature_values[0], 
                                           dewpoint_values[0]).to('degC')

    indices = dict()

    CAPE = mpcalc.cape_cin(pressure_values, temperature_values, dewpoint_values, parcel_profile)#, which_lfc = "top", which_el = "top")
    indices['cape'] = CAPE[0].magnitude
    indices['cin'] = CAPE[1].magnitude

    lift = mpcalc.lifted_index(pressure_values, temperature_values, parcel_profile)
    indices['lift'] = lift[0].magnitude

    k = mpcalc.k_index(pressure_values, temperature_values, dewpoint_values)
    indices['k'] = k.magnitude

    total_totals = mpcalc.total_totals_index(pressure_values, temperature_values, dewpoint_values)
    indices['total_totals'] = total_totals.magnitude

    showalter = mpcalc.showalter_index(pressure_values, temperature_values, dewpoint_values)
    indices['showalter'] = showalter.magnitude[0]

    return indices

def main():
    parser = argparse.ArgumentParser(description='Generate instability indices from sounding measurements.')
    parser.add_argument('--input_file', help='Parquet file name containing the sounding measurements', required=True)
    parser.add_argument('--output_file', help='Parquet file name where the indices are going to be saved', required=True)
    args = parser.parse_args()

    df_s = pd.read_parquet(args.input_file)

    df_indices = pd.DataFrame(columns = ['time', 'cape', 'cin', 'lift', 'k', 'total_totals', 'showalter'])

    for launch_timestamp in pd.to_datetime(df_s.time).unique():
        print(f"Generating instability indices for launch made at {launch_timestamp}...", end="")
        try:
            df_launch = df_s[pd.to_datetime(df_s['time']) == launch_timestamp]
            indices_dict = compute_indices(df_launch)
            indices_dict['time'] = launch_timestamp
            df_indices = pd.concat([df_indices, pd.DataFrame.from_records([indices_dict])])
            print("Success!")
        except ValueError as e:
            print(f'Error processing measurements made by launch at {launch_timestamp}')
            print(f'{repr(e)}')
        except IndexError as e:
            print(f'Error processing measurements made by launch at {launch_timestamp}')
            print(f'{repr(e)}')
        except KeyError as e:
            print(f'Error processing measurements made by launch at {launch_timestamp}')
            print(f'{repr(e)}')

    df_indices.to_parquet(args.output_file, compression='gzip', index = False)

    print("Done!")

if __name__ == "__main__":
    main()
