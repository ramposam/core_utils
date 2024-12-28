import csv
import re

import pandas as pd
import logging
import json

# Mapping from pandas data types to Snowflake data types
PANDAS_TO_SNOWFLAKE_TYPES = {
    "int64": "NUMBER",
    "float64": "FLOAT",
    "bool": "BOOLEAN",
    "datetime64[ns]": "TIMESTAMP",
    "object": "STRING",
    "category": "STRING"
}


def identify_delimiter(file_path):
    # Open the file and read the first 3 lines
    with open(file_path, 'r') as file:
        lines = [file.readline() for _ in range(3)]

    # Join the first 3 lines to form a sample text
    sample = ''.join(lines)

    # Common delimiters to check
    delimiters = [',', ';', '\t', '|']
    delimiter_counts = {delimiter: sample.count(delimiter) for delimiter in delimiters}

    # Find the delimiter with the highest count
    identified_delimiter = max(delimiter_counts, key=delimiter_counts.get)

    return identified_delimiter


def infer_and_convert_data_types(csv_file_path, lines_to_read=5000):
    # Read only the first `lines_to_read` rows of the CSV file
    df = pd.read_csv(csv_file_path, nrows=lines_to_read)

    # Identify the header
    header = [col.replace(" ", "_").upper() for col in list(df.columns)]

    # Infer data types and map to Snowflake data types
    column_data_types = {}
    for column in df.columns:
        pandas_dtype = str(df[column].dtype)
        snowflake_dtype = PANDAS_TO_SNOWFLAKE_TYPES.get(pandas_dtype, "STRING")  # Default to STRING if no match
        column_data_types[column] = {
            "pandas_dtype": pandas_dtype,
            "snowflake_dtype": snowflake_dtype
        }

    return header, column_data_types


def read_and_infer(file_path):
    # Identify the delimiter
    delimiter = identify_delimiter(file_path)
    logging.info(f"Identified delimiter: {delimiter}")

    header, data_types = infer_and_convert_data_types(file_path, lines_to_read=5000)

    return delimiter, header, data_types

def get_unique_keys(file_path,delimiter,header_line,num_rows=5000):

    df = pd.read_csv(file_path, sep=delimiter,header=header_line-1, nrows=num_rows)

    unique_columns = []
    for (col, dtype) in df.dtypes.items():
        unique_columns.append(col)
        max_value = df.value_counts(unique_columns,dropna=False).max()
        if max_value==1:
            break

    unique_keys = [col.replace(" ", "_").upper() for col in unique_columns]

    return unique_keys

def write_to_json_file(data, file_path):
    """
    Writes the given data to a JSON file.

    :param data: The data to write (should be JSON serializable).
    :param file_path: The path to the JSON file.
    """
    try:
        with open(file_path, 'w') as json_file:
            json.dump(data, json_file, indent=4)
        print(f"Data successfully written to {file_path}")
    except Exception as e:
        print(f"An error occurred: {e}")

def write_to_file(data, file_path):
    """
    Writes the given data to a  file.

    """
    try:
        with open(file_path, 'w') as file:
            file.write(data)
        print(f"Data successfully written to {file_path}")
    except Exception as e:
        print(f"An error occurred: {e}")


def get_file_name_pattern(file_name):
  """
  Extracts the filename pattern from a given file name.

  Args:
    file_name: The name of the file.

  Returns:
    A string representing the filename pattern.
    Includes placeholders for datetime components if found.
  """

  pattern = file_name
  date_format = ""
  # Check for datetime patterns
  datetime_match = re.search(r"(\d{4}-\d{2}-\d{2})|(\d{2}-\d{2}-\d{4})|(\d{4}\d{2}\d{2})|(\d{2}\d{2}\d{4})", file_name)
  if datetime_match:
    if re.match(r"\d{4}-\d{2}-\d{2}", datetime_match.group(0)):
      date_format = "%Y-%m-%d"
    elif re.match(r"\d{2}-\d{2}-\d{4}", datetime_match.group(0)):
      date_format = "%d-%m-%Y"
    elif re.match(r"\d{4}\d{2}\d{2}", datetime_match.group(0)):
      date_format = "%Y%m%d"
    elif re.match(r"\d{2}\d{2}\d{4}", datetime_match.group(0)):
      date_format = "%d%m%Y"

    if date_format:
      pattern = re.sub(r"\d{4}-\d{2}-\d{2}", "{datetime_pattern}", pattern)
      pattern = re.sub(r"\d{2}-\d{2}-\d{4}", "{datetime_pattern}", pattern)
      pattern = re.sub(r"\d{4}\d{2}\d{2}", "{datetime_pattern}", pattern)
      pattern = re.sub(r"\d{2}\d{2}\d{4}", "{datetime_pattern}", pattern)

  # Replace other potential variable parts with placeholders
  # (e.g., user IDs, counters)
  # pattern = re.sub(r"[a-zA-Z0-9]{3,}", "{var}", pattern)

  return pattern,date_format
