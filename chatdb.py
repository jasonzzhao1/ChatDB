# main chatdb application

from db_conn import DatabaseConnection
from uploads_analysis import UploadsAnalysis
from sample_query_generator import QueryGenerator
from nlp import NLPProcessor


# function to display query results in a table-like format
def display_results(results, column_names=None):
    if not results:
        print("No results found.")
        return

    col_widths = [max(len(str(value)) for value in col) for col in
                  zip(column_names, *results)]  # define column widths for formatting
    total_width = sum(col_widths) + len(col_widths) * 3 + 1  # adjust for borders and spacing
    print("+" + "-" * (total_width - 2) + "+")  # print the top border using total_width

    if column_names:  # print the header row with borders if column_names are provided
        print("| " + " | ".join(f"{col_name:<{col_widths[idx]}}" for idx, col_name in enumerate(column_names)) + " |")
        print("+" + "-" * (total_width - 2) + "+")  # border after the header
    for row in results:  # print each row with borders
        print("| " + " | ".join(f"{str(value):<{col_widths[idx]}}" for idx, value in enumerate(row)) + " |")
    print("+" + "-" * (total_width - 2) + "+")  # print the bottom border using total_width


# explanation regarding sql constructs
def get_construct_description(construct):
    descriptions = {
        "GROUP BY": "The group by statement groups rows that have the same values into summary rows, like 'total' "
                    "or 'average'. \nIt is often used with aggregate functions like count(), sum(), avg(), etc.",
        "ORDER BY": "The order by clause is used to sort the result set by one or more columns. \nBy default, it "
                    "sorts in ascending order; use 'desc' to sort in descending order.",
        "HAVING": "The having clause is used to filter the results of a group by query. \nIt is similar to the "
                  "where clause but is applied after the grouping operation.",
        "WHERE": "The where clause is used to filter records before any grouping or aggregation occurs. \nIt "
                 "limits the results to only those that meet certain conditions."
    }
    return descriptions.get(construct.upper(), "No description available for this construct.")


# main application interface for chatdb
class ChatDB:

    def __init__(self):
        self.db_connection = DatabaseConnection()  # database connection object
        self.uploads_analysis = UploadsAnalysis(self.db_connection)  # handles user input
        self.query_generator = QueryGenerator(self.db_connection)  # handles sample query-specific operations
        self.nlp_processor = NLPProcessor(self.db_connection)  # natural language processing for user input

    def start(self):
        self.db_connection.connect()
        while True:
            print("ChatDB capabilities:")
            print("1. Upload a dataset")
            print("2. Remove a dataset")
            print("3. Explore specific datasets")
            print("4. Obtain sample queries")
            print("5. Ask inquiries")
            print("Type 'exit' to quit")
            print()
            user_input = input("Type your request here (eg. 1): ").strip()
            print("-" * 300)  # line separator

            if user_input.lower() == 'exit':
                break  # exit out of application
            elif user_input == '1':
                # initial prompt for the table name and file path
                table_name = input("Enter the table name for the dataset: ").strip()
                file_path = input("Enter the path to the CSV file: ").strip()
                print("-" * 300)
                # upload dataset with retry logic in place
                result = self.uploads_analysis.upload_dataset(table_name, file_path)
                if result == "back_to_home":
                    print("-" * 300)
                    continue  # go back to home page after three failed attempts
                print("-" * 300)
            elif user_input == '2':
                self.uploads_analysis.remove_dataset()  # updated method with input handling inside
            elif user_input == '3':
                self.explore_database_tables()  # explore tables
            elif user_input == '4':
                self.display_sample_queries()  # sample queries
            elif user_input == '5':
                self.query_database()  # query database
            else:
                print("Invalid input. Please try again.")  # invalid input
                print("-" * 300)
        self.db_connection.disconnect()  # after breaking, disconnect from database

    # explore database tables
    def explore_database_tables(self):
        while True:  # outer loop to keep the user in the table selection page
            print("Available tables in the database:")
            cursor = self.db_connection.get_cursor()
            cursor.execute("SHOW TABLES;")
            tables = cursor.fetchall()
            cursor.close()

            if not tables:
                print("No tables found in the database.")
                print("-" * 300)
                return

            table_list = [table[0] for table in tables]  # display table options
            for idx, table in enumerate(tables, start=1):
                print(f"- {table[0]}")
            print("-" * 300)

            table_choice = input("Enter the name of the table you want to explore, or type 'back' to return to the "
                                 "main menu: ").strip().lower()
            print("-" * 300)

            if table_choice.lower() == 'back':
                break  # exit to the main menu

            if not table_choice:  # ask for input
                print("Table name cannot be empty. Please try again.")
                print("-" * 300)
                continue

            if table_choice not in table_list:  # invalid table name
                print(f"Invalid table name: '{table_choice}'. Please try again.")
                print("-" * 300)
                continue

            attributes = self.uploads_analysis.get_table_attributes(table_choice)  # fetch and display table attributes
            if not attributes:
                print(f"No attributes found for table: {table_choice}")
                continue

            print(f"You asked to explore the {table_choice} dataset. Here are the table attributes and some sample "
                  f"data!")
            print("\nTable Attributes:")  # show tables attributes
            attributes_str = [f"{column_name} ({data_type})" for column_name, data_type in attributes]
            for i in range(0, len(attributes_str), 8):
                print(", ".join(attributes_str[i:i + 8]))  # show 8 attributes in each row

            sample_data = self.uploads_analysis.get_sample_data(table_choice)  # fetch and display sample data
            print("\nSample Data:")
            if sample_data:
                table_attributes = self.uploads_analysis.get_table_attributes(table_choice)  # get sample data
                column_names = [col[0] for col in table_attributes]  # get column names
                col_widths = [max(len(str(value)) for value in col) for col in zip(*sample_data, *[column_names])]
                total_width = sum(col_widths) + len(col_widths) * 3 + 1
                print("+" + "-" * (total_width - 2) + "+")
                print("| " + " | ".join(
                    f"{col_name:<{col_widths[idx]}}" for idx, col_name in enumerate(column_names)) + " |")
                print("+" + "-" * (total_width - 2) + "+")
                for row in sample_data:
                    print("| " + " | ".join(f"{str(value):<{col_widths[idx]}}" for idx, value in enumerate(row)) + " |")
                print("+" + "-" * (total_width - 2) + "+")  # print sample data
            print("-" * 300)

            while True:  # ask the user if they want to explore another table and loop until a valid response is given
                explore_another = input("Do you want to explore another table? (yes/no): ").strip().lower()
                print("-" * 300)

                if explore_another == 'yes':
                    break  # continue the loop to explore another table
                elif explore_another == 'no':
                    return  # exit to the main menu
                else:
                    print("Invalid input. Please enter 'yes' or 'no'.")
                    print("-" * 300)

    # display dynamically generated sample queries with or without construct for a selected table
    def display_sample_queries(self):
        while True:
            print("Available tables in the database:")
            cursor = self.db_connection.get_cursor()
            cursor.execute("SHOW TABLES;")
            tables = cursor.fetchall()
            cursor.close()

            if not tables:
                print("No tables found in the database.")
                print("-" * 300)
                return

            table_list = [table[0] for table in tables]
            for table_name in table_list:
                print(f"- {table_name}")
            print("-" * 300)

            # prompt user to select a table and optionally specify a construct
            print("Example input: 'supermarket' (no construct) or 'nba group by'")
            user_input = input("Enter the table name followed by the construct (optional), or type 'back' to return "
                               "to the main menu: ").strip().lower()
            print("-" * 300)

            if user_input.lower() == 'back':
                break

            parts = user_input.split(" ", 1)  # split the input into components (table name and construct if specified)
            table_choice = parts[0]
            construct = parts[1].upper() if len(parts) > 1 else None  # convert construct to uppercase if specified

            if table_choice not in table_list:  # check if the table exists
                print(f"Table '{table_choice}' not found.")
                print("-" * 300)
                continue  # return to table selection

            description_displayed = False  # flag to track if description has been shown

            while True:  # inner loop to display queries for the selected table
                if construct:  # generate queries based on user input
                    if not description_displayed:  # display description for the construct only once
                        print(f"You have selected to include '{construct}'!")
                        print(f"{get_construct_description(construct)}")
                        print()
                        description_displayed = True  # set flag to true after showing the description
                        # generate queries for the specified construct
                    construct_queries = self.query_generator.generate_queries_by_construct(table_choice, construct)
                    if not construct_queries:
                        print(f"No sample queries available for '{construct}' on table: {table_choice}")
                        print("-" * 300)
                        break

                    print(f"Sample queries with '{construct}' for the {table_choice} dataset")
                    for query_info in construct_queries:
                        print(f"\nDescription: {query_info['description']}")
                        print(f"Query: {query_info['query']}")

                else:
                    # generate random sample queries without a specified construct
                    random_queries = self.query_generator.generate_systematic_queries(table_choice)
                    if not random_queries:
                        print(f"No random sample queries available for table: {table_choice}")
                        print("-" * 300)
                        break
                    print(f"Here are three sample queries for the {table_choice} dataset:")
                    for query_info in random_queries:
                        print(f"\nDescription: {query_info['description']}")
                        print(f"Query: {query_info['query']}")
                print("-" * 300)

                while True:  # ask the user if they want more sample queries for the same table
                    more_queries = input("Do you want more sample queries for this table/construct? "
                                         "(yes/no): ").strip().lower()
                    print("-" * 300)
                    if more_queries == "yes":
                        break  # continue to show more queries
                    elif more_queries == "no":
                        break  # exit to table selection page
                    else:
                        print("Invalid input. Please specify 'yes' or 'no'.")  # invalid input handling
                        print("-" * 300)

                if more_queries == "no":
                    break  # break out of the inner loop and go back to table selection

    # handle database querying
    def query_database(self):
        while True:
            print("Available tables in the database:")
            cursor = self.db_connection.get_cursor()
            cursor.execute("SHOW TABLES;")
            tables = cursor.fetchall()
            cursor.close()

            if not tables:
                print("No tables found in the database.")
                print("-" * 300)
                break

            table_list = [table[0].lower() for table in tables]
            for table_name in table_list:
                print(f"- {table_name}")
            print("-" * 300)

            table_name = input("Enter the name of the table you want to query, or type 'back' to return to the "
                               "main menu: ").strip().lower()  # prompt the user to select a table by name
            print("-" * 300)

            if table_name.lower() == 'back':  # user wants to go back to home page
                break

            if not table_name:  # empty table input
                print("Table name cannot be empty. Please try again.")
                print("-" * 300)
                continue

            if table_name not in table_list:  # validate the table choice
                print(f"Invalid table name: '{table_name}'. Please try again.")
                print("-" * 300)
                continue

            self.process_query(table_name)  # call on function to process the user's question for the selected table

    # process user questions and execute appropriate queries based on the selected table
    def process_query(self, table_name):
        while True:
            print("Please type out all inquiries as accurately as possible. Check for all spelling errors!")
            print("Reference exact column/data names instead of using synonymous terms.")
            user_input = input(f"Ask your question for table '{table_name}', or type 'back' to return to the "
                               f"main menu: ").strip()

            if user_input.lower() == 'back':
                print("-" * 300)
                break

            # extract intent and generate sql query
            intent_data = self.nlp_processor.extract_intent(user_input, table_name)

            if "error" in intent_data:  # handle errors returned by the nlpprocessor
                print(f"Error: {intent_data['error']}")
                print("-" * 300)
                continue

            sql_query = intent_data.get("sql_query")  # extract the sql query and description
            description = intent_data.get("description")

            cursor = None  # execute the query
            try:
                if not sql_query:  # ensure that query is not None or empty
                    print("Error: No query generated.")
                    continue
                cursor = self.db_connection.get_cursor()
                cursor.execute(sql_query)
                results = cursor.fetchall()
                column_names = [desc[0] for desc in cursor.description]  # extract column names for display
                print("-" * 300)
                print(f"You asked to {description}.")
                print("\nThis is the corresponding SQL query: " + "\033[1m" + f"{sql_query};" + "\033[0m")
                print("\nQuery results:")
                display_results(results, column_names)
            except Exception as e:
                print(f"Error executing query: {e}")
            finally:
                if cursor:
                    cursor.close()
            print("-" * 300)
