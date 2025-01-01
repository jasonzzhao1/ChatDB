# handles dataset uploads and exploratory data analysis

import re
import os
import csv
import pandas as pd


class UploadsAnalysis:

    def __init__(self, db_connection):
        self.db_connection = db_connection

    # method to classify uploaded datasets without predefined data types
    @staticmethod
    def infer_column_type(sample_values):
        is_int = True  # initialize type flags
        is_decimal = True
        is_date = True
        is_text = False

        date_pattern = re.compile(r"^\d{4}-\d{2}-\d{2}$|^\d{2}/\d{2}/\d{4}$")  # regex pattern for date types

        for value in sample_values:  # loop through values
            value = value.strip()  # strip whitespace
            if not value:  # skip empty values
                continue
            try:  # check if value can be an integer
                int(value)
            except ValueError:
                is_int = False
            try:  # check if value can be a decimal/float
                float(value)
            except ValueError:
                is_decimal = False
            if not date_pattern.match(value):  # check if value can be a date
                is_date = False
            else:
                try:
                    pd.to_datetime(value, errors="coerce", format="%Y-%m-%d")  # strict parsing
                except ValueError:
                    is_date = False
            if not is_int and not is_decimal and not is_date:  # check if the value is likely text
                is_text = True
            if not is_int and not is_decimal and not is_date and not is_text:  # exit early if none of the types match
                break

        if is_int:  # return the column type based on the flags
            return "INT"
        elif is_decimal:
            return "DECIMAL(20, 6)"
        elif is_date:
            return "DATE"
        elif is_text:
            max_length = max(len(str(val)) for val in sample_values if val.strip() != "")
            return f"VARCHAR({max_length if max_length > 0 else 255})"  # set varchar as the longest text in sample data
        else:
            return "VARCHAR(255)"  # default to varchar if no specific type is determined

    # create table from csv file, dynamically detecting column types
    def create_table_from_csv(self, table_name, csv_file_path):
        with open(csv_file_path, 'r') as file:  # open csv file
            csv_reader = csv.reader(file)  # read
            header = next(csv_reader)  # read the header row
            sample_rows = [row for _, row in zip(range(100), csv_reader)]  # read up to 100 sample rows

        column_types = {}  # infer column types
        for i, column_name in enumerate(header):  # loop through values
            sample_values = [row[i] for row in sample_rows if len(row) > i]
            column_types[column_name] = self.infer_column_type(sample_values)

        columns = ", ".join([f"`{col}` {dtype}" for col, dtype in column_types.items()])  # join types
        create_table_query = f"CREATE TABLE IF NOT EXISTS `{table_name}` ({columns});"  # construct create table query
        cursor = self.db_connection.get_cursor()
        cursor.execute(create_table_query)
        cursor.close()

    # upload dataset from csv file to a specific tables, returns user to home page after three failed uploads
    def upload_dataset(self, table_name, csv_file_path):
        error_count = 0  # initialize error count
        while error_count < 3:  # allow three upload attempts
            conn = self.db_connection.connection  # get the database connection
            cursor = conn.cursor()  # get the cursor
            try:
                if not os.path.exists(csv_file_path):  # check if the file exists
                    raise FileNotFoundError(
                        f"No such file or directory: '{csv_file_path}'")  # raise an error if not found
                self.create_table_from_csv(table_name, csv_file_path)  # create table dynamically
                with open(csv_file_path, "r") as file:  # open csv file
                    csv_reader = csv.reader(file)
                    next(csv_reader)  # skip the header row
                    for idx, row in enumerate(csv_reader, start=1):  # process each row
                        placeholders = ", ".join(["%s"] * len(row))  # create placeholders for row data
                        try:
                            cursor.execute(f"INSERT INTO `{table_name}` VALUES ({placeholders})", row)  # insert row
                        except Exception as row_error:  # catch row-specific errors
                            print(f"Error inserting row {idx}: {row_error}")  # log the error
                conn.commit()  # commit all successful inserts
                print(f"Dataset uploaded successfully into table '{table_name}'!")  # success message
                return

            except Exception as e:
                print(f"An error occurred while uploading the dataset: {e}")  # print error statement
                error_count += 1  # increase error count
                conn.rollback()
                if error_count < 3:  # if less than 3 errors, allow user to try again
                    print(f"Please try again! You have {3 - error_count} attempts left.")
                    print("-" * 300)
                    # ask the user to re-enter file path and table name
                    table_name = input("Enter the table name for the dataset: ").strip()
                    csv_file_path = input("Enter the path to the CSV file: ").strip()
                    print("-" * 300)
                else:  # once limit is hit, return to home page
                    print("Oops! It seems as if you've reached the maximum amount of failed attempts. I will now "
                          "return you back to the home page.")
                    return "back_to_home"

            finally:
                cursor.close()

        return None  # return none if the loop ends without reaching 'back_to_home'

    # method to remove a dataset from the database
    def remove_dataset(self):
        while True:  # keep looping until the user either removes a table or goes back
            print("Available tables in the database:")
            cursor = self.db_connection.get_cursor()
            cursor.execute("SHOW TABLES;")
            tables = cursor.fetchall()
            cursor.close()

            if not tables:
                print("No tables found in the database.")
                print("-" * 300)
                return  # if no tables are available, exit the method

            table_list = [table[0] for table in tables]
            for table_name in table_list:
                print(f"- {table_name}")
            print("-" * 300)

            user_input = input(
                "Enter the table name to remove, or type 'back' to return to the main menu: ").strip().lower()
            print("-" * 300)

            if user_input == 'back':
                break  # exit to the home page

            if user_input not in table_list:
                print(f"Table '{user_input}' not found. Try again!")
                print("-" * 300)
                continue  # retry the operation

            cursor = self.db_connection.get_cursor()  # proceed to remove the dataset
            try:
                drop_table_query = f"DROP TABLE IF EXISTS `{user_input}`;"  # prepare query to drop table
                cursor.execute(drop_table_query)
                self.db_connection.connection.commit()
                print(f"Table '{user_input}' has been removed from the database.")
                print("-" * 300)
                break  # exit after successful deletion

            except Exception as e:
                print(f"An error occurred while removing the dataset: {e}")
                print("-" * 300)
                break  # exit on error
            finally:
                cursor.close()  # always close the cursor

    # fetches the column names and their data types for a given table
    def get_table_attributes(self, table_name):
        cursor = self.db_connection.get_cursor()  # connect to database
        query = f"""
        SELECT COLUMN_NAME, DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = %s AND TABLE_SCHEMA = %s; 
        """
        cursor.execute(query, (table_name, self.db_connection.database))
        results = cursor.fetchall()  # select attributes
        cursor.close()
        return results

    # fetches sample rows from a given table
    def get_sample_data(self, table_name, limit=5):
        cursor = self.db_connection.get_cursor()
        query = f"SELECT * FROM {table_name} LIMIT %s;"  # select all data from table
        cursor.execute(query, (limit,))
        results = cursor.fetchall()
        cursor.close()
        return results
