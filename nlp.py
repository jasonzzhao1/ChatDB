# nlp capabilities for user inquiries

import re
import string


class NLPProcessor:
    def __init__(self, db_connection):
        self.db_connection = db_connection  # initialize with db connection

        # dictionary to convert number words to digits
        self.number_words_to_digits = {
            "zero": "0", "one": "1", "two": "2", "three": "3", "four": "4",
            "five": "5", "six": "6", "seven": "7", "eight": "8", "nine": "9",
            "ten": "10", "eleven": "11", "twelve": "12", "thirteen": "13",
            "fourteen": "14", "fifteen": "15", "sixteen": "16", "seventeen": "17",
            "eighteen": "18", "nineteen": "19", "twenty": "20"
        }

    # preprocess user input by normalizing and handling synonyms
    def preprocess_input(self, user_input):
        # synonym mappings to replace common phrases with column names or sql terms
        synonym_map = {
            # nba specific synonyms
            "players": "player_name", "how tall": "player_height", "how heavy": "player_weight",
            "performance": "ppg apg net_rating", "statistics": "ppg apg rpg net_rating oreb_percent dreb_percent",
            "points": "ppg", "rebounds": "rpg", "assists": "apg", "scoring": "ppg", "season": "season",
            "draft year": "draft_year", "drafted in": "draft_year",
            # netflix specific synonyms
            "shows": "title", "movies": "type", "genres": "listed_in", "ratings": "rating",
            # supermarket specific synonyms
            "sales": "total", "products": "product_line", "cost": "cogs", "profit": "gross_income",
            "customer type": "customer_type", "payment method": "payment",
            # general synonyms
            "best": "MAX", "worst": "MIN", "average": "AVG", "total": "SUM", "sum": "SUM", "greater than": ">",
            "greater than or equal to": ">=", "less than": "<", "less than or equal to": "<="
        }

        # normalize input by converting to lowercase, removing punctuation, and replacing synonyms
        user_input = user_input.lower()
        user_input = user_input.translate(str.maketrans("", "", string.punctuation.replace("-", "").replace("_", "")))
        user_input = re.sub(r"(\d{4})(\d{2})", r"\1-\2", user_input)

        # replace synonyms with mapped terms
        for phrase, replacement in synonym_map.items():
            user_input = user_input.replace(phrase, replacement)

        # split the input into tokens and convert number words to digits
        tokens = user_input.split()
        tokens = [self.number_words_to_digits.get(token, token) for token in tokens]

        return tokens  # return preprocessed tokens

    # fetch column mappings from the database for a given table
    def fetch_column_mapping(self, table_name):
        cursor = self.db_connection.get_cursor()
        cursor.execute(f"SHOW COLUMNS FROM {table_name};")
        columns = cursor.fetchall()
        cursor.close()

        quantitative_columns = []  # initialize list for quantitative columns
        categorical_columns = []  # initialize list for categorical columns

        # iterate over columns and classify them by data type
        for column in columns:
            column_name = column[0]
            data_type = column[1]
            base_type = data_type.split('(')[0]  # get the base type
            if base_type in ["int", "decimal", "double", "bigint"]:
                quantitative_columns.append(column_name)  # add to quantitative list if numeric
            elif base_type in ["varchar", "mediumtext", "char", "date", "time"]:
                categorical_columns.append(column_name)  # add to categorical list if text/date

        return {"quantitative": quantitative_columns,
                "categorical": categorical_columns}  # return column classification

    # handle special conditions like draft year and season
    @staticmethod
    def handle_special_conditions(tokens, i, conditions):
        # handle draft year references and ensure valid year format
        if tokens[i] == "draft_year" and i + 1 < len(tokens):
            draft_year = tokens[i + 1]
            if draft_year.isdigit() and len(draft_year) == 4:  # check if year is valid
                conditions.append(f"draft_year = '{draft_year}'")  # add to conditions
                return i + 1  # skip the next token (the year)

        # handle season references and ensure valid season format
        if tokens[i] == "season" and i > 0:
            season = tokens[i - 1]
            if re.match(r"^\d{4}-\d{2}$", season):  # match season format
                conditions.append(f"season = '{season}'")  # add to conditions
                return i  # return the current index

        return i  # return unchanged index if no special condition matched

    # handle column-value conditions
    @staticmethod
    def handle_column_value_conditions(tokens, i, column_mapping, conditions):
        # handle conditions like column = value, column > value
        if i + 2 < len(tokens) and tokens[i + 1] in {"is", "equals", "greater", "less", ">", "<", "between"}:
            column = tokens[i]
            operator = tokens[i + 1]
            value = tokens[i + 2]

            condition_map = {  # map operator words to sql operators
                "greater": ">", "greater than": ">", "less": "<", "less than": "<",
                "equals": "=", "equal to": "=",
                ">": ">", "<": "<"
            }
            sql_operator = condition_map.get(operator)
            if sql_operator and column in column_mapping["quantitative"]:  # check if valid column
                conditions.append(f"{column} {sql_operator} {value}")  # add condition to list
                return i + 2  # skip the current token, operator, and value

        # handle conditions with "fewer than" or "more than"
        if tokens[i] in column_mapping["quantitative"] and i + 3 < len(tokens):
            column = tokens[i]
            if tokens[i + 1] in {"fewer", "more"} and tokens[i + 2] == "than":
                value = tokens[i + 3]
                if tokens[i + 1] == "fewer":
                    conditions.append(f"{column} < {value}")
                elif tokens[i + 1] == "more":
                    conditions.append(f"{column} > {value}")
                return i + 3

        # handle "between" conditions
        if tokens[i] in column_mapping["quantitative"] and i + 4 < len(tokens):
            column = tokens[i]
            if tokens[i + 1] == "between" and tokens[i + 3] == "and":
                value1 = tokens[i + 2]
                value2 = tokens[i + 4]
                conditions.append(f"{column} BETWEEN {value1} AND {value2}")
                return i + 4  # skip the "between" and "and" parts

        return i  # return unchanged index if no condition matched

    # handle aggregation functions
    @staticmethod
    def handle_aggregation(token, aggregation):
        aggregation_map = {
            "average": "AVG", "avg": "AVG", "AVG": "AVG",
            "maximum": "MAX", "max": "MAX", "MAX": "MAX",
            "minimum": "MIN", "min": "MIN", "MIN": "MIN",
            "sum": "SUM", "SUM": "SUM", "total": "SUM", "TOTAL": "SUM",
            "count": "COUNT"
        }
        return aggregation_map.get(token, aggregation)  # return the mapped aggregation function

    # handle sorting tokens
    @staticmethod
    def handle_sorting(tokens, i, order_by):
        if tokens[i] == "highest" and i + 1 < len(tokens):
            return f"{tokens[i + 1]} DESC"  # descending order
        elif tokens[i] == "lowest" and i + 1 < len(tokens):
            return f"{tokens[i + 1]} ASC"  # ascending order
        return order_by  # return unchanged order if no sorting matched

    # handle limit tokens
    @staticmethod
    def handle_limit(tokens, i, limit):
        if tokens[i] == "top" and i + 1 < len(tokens) and tokens[i + 1].isdigit():
            return int(tokens[i + 1])  # return limit as integer
        return limit  # return unchanged limit if no "top" condition matched

    # match preprocessed tokens to sql query components
    def match_tokens_to_sql(self, tokens, column_mapping):
        action = "SELECT"  # default action is select
        columns = []  # initialize list for selected columns
        conditions = []  # initialize list for conditions
        group_by = []  # initialize list for group by
        order_by = None  # initialize order by
        limit = None  # initialize limit
        aggregation = None  # initialize aggregation function

        i = 0
        while i < len(tokens):  # iterate over all tokens
            token = tokens[i]
            aggregation = self.handle_aggregation(token, aggregation)  # handle aggregation

            # add the token to the columns if it's a valid column name
            if token in column_mapping["quantitative"] or token in column_mapping["categorical"]:
                if token not in columns:
                    columns.append(token)

            i = self.handle_special_conditions(tokens, i, conditions)  # handle special conditions
            i = self.handle_column_value_conditions(tokens, i, column_mapping,
                                                    conditions)  # handle column-value conditions
            order_by = self.handle_sorting(tokens, i, order_by)  # handle sorting
            limit = self.handle_limit(tokens, i, limit)  # handle limit

            i += 1  # move to next token

        # ensure group by is set if aggregation exists
        if aggregation and not group_by:
            group_by = columns

        # return all components of the sql query
        return {
            "action": action,
            "columns": columns,
            "conditions": " AND ".join(conditions),
            "group_by": group_by,
            "limit": limit,
            "order_by": order_by,
            "aggregation": aggregation
        }

    # generate the final sql query based on components
    @staticmethod
    def generate_query(components, table_name):
        query = f"{components['action']} "
        if components["aggregation"]:
            query += f"{components['aggregation']}({components['columns'][0]})"  # apply aggregation to first column
        else:
            query += f"{', '.join(components['columns'])}" if components["columns"] else "*"
        query += f" FROM {table_name}"
        if components["conditions"]:
            query += f" WHERE {components['conditions']}"
        if components["group_by"]:
            query += f" GROUP BY {', '.join(components['group_by'])}"
        if components["order_by"]:
            query += f" ORDER BY {components['order_by']}"
        if components["limit"]:
            query += f" LIMIT {components['limit']}"
        return query  # return generated query string

    # process user input and extract intent
    def extract_intent(self, user_input, table_name):
        if user_input.strip().lower().startswith("select"):  # handle raw sql input
            return {
                "description": f"execute your query in the {table_name} dataset",
                "sql_query": user_input.strip()  # use the raw query directly
            }

        # process the user input to generate a sql query
        tokens = self.preprocess_input(user_input)
        column_mapping = self.fetch_column_mapping(table_name)
        components = self.match_tokens_to_sql(tokens, column_mapping)

        if not components["columns"] and not components["aggregation"]:
            return {"error": "Could not identify columns or aggregation in your query."}

        try:
            sql_query = self.generate_query(components, table_name)
            return {
                "description": f"query {', '.join(components['columns'])} with these filters: "
                               f"{components['conditions']}",
                "sql_query": sql_query
            }
        except Exception as e:
            return {"error": str(e)}  # handle errors during query generation
