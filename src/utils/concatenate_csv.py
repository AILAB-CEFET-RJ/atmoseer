import sys
import os
import csv

# Define usage message
usage_msg = 'Usage: python concat_csv.py <input_dir> <output_file>'

# Get command line arguments
if len(sys.argv) != 3:
    print(usage_msg)
    sys.exit(2)
else:
    input_dir = sys.argv[1]
    output_file = sys.argv[2]

# Initialize list to hold rows from input files.
rows = []

is_first_file = True

# Loop through input files and append rows to list
for filename in os.listdir(input_dir):
    if filename.endswith('.csv'):
        with open(os.path.join(input_dir, filename), newline='') as csvfile:
            reader = csv.reader(csvfile)
            if is_first_file:
                is_first_file = False
            else:
                next(reader) # This skips the first row of the CSV file (because it is the header)
            for row in reader:
                rows.append(row)

# Write rows to output file
with open(output_file, 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    for row in rows:
        writer.writerow(row)

print(f'{len(rows)} rows written to {output_file}.')

# python concatenate_csv.py input_dir SBGL_1997-01-01_2022-12-31.csv
