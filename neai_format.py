"""
Python script to convert data logging file into signal usable with NanoEdge AI Studio.

USAGE:
    python3 neai_format.py -i <input_file.csv> -o <output_file.csv>

    where:
       - <input_file.csv> is your data log input file, must be text or csv
       - <output_file.csv> is your output_file in .csv format

Authors: Cartesiam Team
Copyright: Copyright 2020, Cartesiam
"""

"""
PARAMETERS TO SET:
"""

# INPUT FILE
INPUT_FILE_VALUE_DELIMITER = ','            # Specify the values delimiter of input file
INPUT_FILE_DECIMAL_DELIMITER = '.'          # Specify the decimal delimiter of input file
INPUT_FILE_HAS_HEADERS = False              # True, if input file has headers, False otherwise

# DATA LOGGING
COLUMNS_TO_KEEP = [2, 3] # or ['X', 'Y']    # columns' indexes or labels to keep (starting at index 1)
BUFFER_SIZE = 128                           # number of values per axis, per line (nb of samples per signal)
DOWNSAMPLE_FACTOR = 1                       # take every (1) or every other (2) or every third (3) line in the data log.

# OUTPUT FILE
LINES_TO_BUILD = 'ALL'                      # number of signal examples to create (integer nb of lines or 'ALL')

""" 
This script creates a dataset with appropriate format for NanoEdge AI Studio:
    For every data line (unless downsampling >1),
    capture only the data corresponding to the given columns.
    Then convert these data from line to column, within the defined 'BUFFER_SIZE' limit.
    Repeat until 'LINES_TO_BUILD' lines have been written.
The output file will have 'LINES_TO_BUILD' lines, each containing 'length of COLUMNS_TO_KEEP'x'BUFFER_SIZE' values per line.
"""

"""
DO NOT MODIFY BELOW
"""

from argparse import ArgumentParser
import logging
import os


def read_args():
    """Read arguments from command line"""
    parser = ArgumentParser(description="NEAI Formatter")
    parser.add_argument('-i', '--input_file', help='Input file path', required=True)
    parser.add_argument('-o', '--output_file', help='Output file path', required=True)
    return parser.parse_args()


def read_values_of_line(line):
    """Read values in line. Line is splitted by INPUT_FILE_VALUE_DELIMITER."""
    if INPUT_FILE_VALUE_DELIMITER == INPUT_FILE_DECIMAL_DELIMITER:
        exit_on_error(f"Input file value delimiter and decimal delimiter are equal. Please set INPUT_FILE_VALUE_DELIMITER and INPUT_FILE_DECIMAL_DELIMITER.")

    # Clean line
    line = line.rstrip('\n').rstrip('\r')

    # Split line by value delimiter
    values = line.split(INPUT_FILE_VALUE_DELIMITER)
    return values


def convert_to_float(value_str, line_number=None, raise_error=False):
    """Convert value to float"""
    # Change decimal delimiter
    float_str = value_str.replace(INPUT_FILE_DECIMAL_DELIMITER, '.')
    # Try float conversion
    try:
        return float(float_str)
    except ValueError:
        if raise_error:
            raise
        elif line_number == 1 and not INPUT_FILE_HAS_HEADERS:
            exit_on_error(f'''Cannot convert {float_str} to float. Original value is {value_str} at line {line_number}. 
       It may be because your input file has headers. In that case please set INPUT_FILE_HAS_HEADERS to True.
       Or it may be because values and decimal delimiters are not correctly set. Please check INPUT_FILE_VALUE_DELIMITER and INPUT_FILE_DECIMAL_DELIMITER.''')
        elif line_number == 2 and INPUT_FILE_HAS_HEADERS:
            exit_on_error(f'''Cannot convert {float_str} to float. Original value is {value_str} at line {line_number}. 
       It may be because value and decimal delimiters are not correctly set. Please check INPUT_FILE_VALUE_DELIMITER and INPUT_FILE_DECIMAL_DELIMITER.''')
        else:
            exit_on_error(f'Cannot convert {float_str} to float. Original value is {value_str} at line {line_number}.')
            
            
def format_list(list_to_format):
    return '  '.join([f'{val_idx + 1}: {val}' for val_idx, val in enumerate(list_to_format)])


def get_headers(file):
    """Read headers of file, if exists"""
    if not isinstance(INPUT_FILE_HAS_HEADERS, bool):
        exit_on_error(f'Parameter INPUT_FILE_HAS_HEADERS must be True or False')

    if not INPUT_FILE_HAS_HEADERS:
        return None

    # Read first line
    first_line = file.readline()
    first_line_values = read_values_of_line(first_line)
    
    # Check if headers may be float, in that case INPUT_FILE_HAS_HEADERS may be not be correctly set
    headers_is_float = True
    try:
        convert_to_float(first_line_values[-1], raise_error=True)
    except ValueError:
        headers_is_float = False
    if headers_is_float:
        logging.warning('Input file headers contain float value. Please check if input file contains headers. If no, please set INPUT_FILE_HAS_HEADERS to False')

    # Return headers
    return first_line_values


def get_columns_indexes_to_keep(headers):
    """Return columns indexes to keep"""
    if not COLUMNS_TO_KEEP:
        exit_on_error('Please set parameter COLUMNS_TO_KEEP')

    columns_indexes = []

    # If COLUMNS_TO_KEEP is a list of integer, i.e. indexes
    if all([isinstance(column, int) for column in COLUMNS_TO_KEEP]):
        columns_indexes = []
        for column_number in COLUMNS_TO_KEEP:
            columns_indexes.append(int(column_number) - 1)
    # Else COLUMNS_TO_KEEP is a list of labels
    else:
        if not INPUT_FILE_HAS_HEADERS:
            exit_on_error(f'Your file has no headers (parameter INPUT_FILE_HAS_HEADERS is False) and COLUMNS_TO_KEEP contains column labels')
        for label in COLUMNS_TO_KEEP:
            try:
                columns_indexes.append(headers.index(label))
            except ValueError:
                exit_on_error(f'Column label "{label}" cannot be found in input file headers')
    return columns_indexes


def create_dataset(input_file_reader, output_file_path, columns_indexes):
    """Create the dataset and write it to output file"""
    nb_lines_built = 0
    tmp_buffer = []

    # For each line in the input file
    for line_index, line in enumerate(input_file_reader, start=int(INPUT_FILE_HAS_HEADERS) + 1):

        # Stop if LINES_TO_BUILD reached
        if isinstance(LINES_TO_BUILD, int) and nb_lines_built >= LINES_TO_BUILD:
            break

        # Read values in line
        values = read_values_of_line(line)
        nb_values = len(values)

        # Build buffer
        if line_index % DOWNSAMPLE_FACTOR == 0:
            for idx in columns_indexes:
                if nb_values <= idx:
                    if line_index <= 2:
                        exit_on_error(f'''Line {line_index} does not contain enough values. Looking for columns number {idx + 1} and only {nb_values} columns / {format_list(values)}
       It may be because values delimiter is not correctly set. Please check INPUT_FILE_VALUE_DELIMITER and INPUT_FILE_DECIMAL_DELIMITER.''')
                    else:
                        exit_on_error(f'Line {line_index} does not contain enough values. Looking for columns number {idx + 1} and only {nb_values} columns / {format_list(values)}')
                value_float = convert_to_float(values[idx], line_number=line_index)
                tmp_buffer.append(str(value_float))

        # Write line if buffer complete
        if len(tmp_buffer) / len(columns_indexes) == BUFFER_SIZE:
            # Write line to output file
            output_line_string = ','.join(tmp_buffer)
            with open(output_file_path, "a") as o:
                nl = '\n'
                o.write(f'{nl if nb_lines_built > 0 else ""}' + output_line_string)
            # Set counters
            tmp_buffer = []
            nb_lines_built += 1

            # Log progress
            if nb_lines_built % 50 == 0:
                print('\rINFO: Number of samples wrote: %3d' % nb_lines_built, end='', flush=True)

    print('\rINFO: Number of samples wrote: %3d' % nb_lines_built, end='', flush=True)
    print()

    logging.info(f'Output file successfully created with {nb_lines_built} samples of length {len(columns_indexes) * BUFFER_SIZE}')


def exit_on_error(error_message):
    logging.error(error_message)
    exit(1)


"""
MAIN SEQUENCE
"""
if __name__ == '__main__':

    logging.basicConfig(format='%(levelname)s: %(message)s', datefmt='%Y/%m/%d %H:%M:%S', level=logging.INFO)

    args = read_args()

    # Check that input file exists
    if not os.path.isfile(args.input_file):
        exit_on_error(f'Input file {args.input_file} does not exist.')

    # Check that output file does not exist
    if os.path.isfile(args.output_file):
        exit_on_error(f'Output file {args.output_file} already exist.')

    # Open file, treat log data, and write output file
    with open(args.input_file, 'r', newline='') as input_file:
        # Get input file headers if any
        headers = get_headers(input_file)
        if headers:
            logging.info(f'Input file has headers: {format_list(headers)}')

        # Get columns indexes to keep
        columns_indexes_to_keep = get_columns_indexes_to_keep(headers)

        # Create dataset
        create_dataset(input_file,
                       args.output_file,
                       columns_indexes_to_keep)

    exit(0)
