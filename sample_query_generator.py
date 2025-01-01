# handles sample query construction based on recognized patterns

import random


class QueryGenerator:

    def __init__(self, db_connection):
        self.db_connection = db_connection

    # classifies columns into quantitative and categorical attributes based on their data type
    def classify_columns(self, table_name):
        cursor = self.db_connection.get_cursor()
        cursor.execute(f"SHOW COLUMNS FROM {table_name};")
        columns = cursor.fetchall()
        cursor.close()

        quantitative_columns = []  # initialize empty list to hold quantitative columns
        categorical_columns = []  # initialize empty list to hold categorical columns

        # iterate through all columns, sorting by datatype
        for column in columns:
            column_name = column[0]
            data_type = column[1]
            base_type = data_type.split('(')[0]  # get the base type
            if base_type in ["int", "decimal", "double", "bigint"]:
                quantitative_columns.append(column_name)  # add to quantitative list if numeric
            elif base_type in ["varchar", "mediumtext", "char", "date", "time"]:
                categorical_columns.append(column_name)  # add to categorical list if text/date

        return quantitative_columns, categorical_columns  # return classified column lists

    # generate systematic queries based on table columns and common sql patterns
    def generate_systematic_queries(self, table_name):
        quantitative_columns, categorical_columns = self.classify_columns(table_name)  # classify columns

        query_templates = [  # initialize query templates for common patterns
            {"pattern": "Total <A> by <B>",
             "sql_template": "SELECT <B>, SUM(<A>) AS total_<A> FROM {table_name} GROUP BY <B>"},

            {"pattern": "Average <A> by <B>",
             "sql_template": "SELECT <B>, AVG(<A>) AS average_<A> FROM {table_name} GROUP BY <B>"},

            {"pattern": "Count <B>",
             "sql_template": "SELECT <B>, COUNT(*) AS count FROM {table_name} GROUP BY <B>"},

            {"pattern": "Minimum <A> by <B>",
             "sql_template": "SELECT <B>, MIN(<A>) AS min_<A> FROM {table_name} GROUP BY <B>"},

            {"pattern": "Maximum <A> by <B>",
             "sql_template": "SELECT <B>, MAX(<A>) AS max_<A> FROM {table_name} GROUP BY <B>"},

            {"pattern": "Select all records",
             "sql_template": "SELECT * FROM {table_name}"},

            {"pattern": "Distinct values of <B>",
             "sql_template": "SELECT DISTINCT <B> FROM {table_name}"}
        ]

        queries = []  # initialize empty query list to store output
        # iterate over quantitative and categorical columns to generate queries
        for quantitative in quantitative_columns:
            for categorical in categorical_columns:
                for pattern in query_templates:  # loop through all pattern templates
                    if "<A>" not in pattern["sql_template"] and "<B>" not in pattern["sql_template"]:
                        # if no placeholders <A> or <B> are in the pattern, format directly (handles select *)
                        query = pattern["sql_template"].format(table_name=table_name)
                        natural_language = pattern["pattern"]
                        queries.append({"description": natural_language, "query": query})
                    else:
                        # replace <A> and <B> placeholders with actual columns
                        query = pattern["sql_template"].replace("<A>", quantitative).replace("<B>", categorical)
                        query = query.format(table_name=table_name)
                        natural_language = pattern["pattern"].replace("<A>", quantitative).replace("<B>", categorical)
                        queries.append({"description": natural_language, "query": query})

        unique_queries = {query["query"]: query for query in queries}  # ensure queries are unique
        sampled_queries = random.sample(list(unique_queries.values()), min(3, len(unique_queries)))
        return sampled_queries  # return list of random queries

    # generate sample queries based on the specified SQL construct (GROUP BY, ORDER BY, etc.)
    def generate_queries_by_construct(self, table_name, construct):
        quantitative_columns, categorical_columns = self.classify_columns(table_name)  # classify columns

        query_templates = {
            "GROUP BY": [  # templates for GROUP BY queries
                {"pattern": "Total <A> by <B>",
                 "sql_template": "SELECT <B>, SUM(<A>) AS total_<A> FROM {table_name} GROUP BY <B>"},

                {"pattern": "Count <B>",
                 "sql_template": "SELECT <B>, COUNT(*) AS count FROM {table_name} GROUP BY <B>"},

                {"pattern": "Average <A> by <B>",
                 "sql_template": "SELECT <B>, AVG(<A>) AS average_<A> FROM {table_name} GROUP BY <B>"},

                {"pattern": "Minimum <A> by <B>",
                 "sql_template": "SELECT <B>, MIN(<A>) AS min_<A> FROM {table_name} GROUP BY <B>"},

                {"pattern": "Maximum <A> by <B>",
                 "sql_template": "SELECT <B>, MAX(<A>) AS max_<A> FROM {table_name} GROUP BY <B>"}
            ],

            "ORDER BY": [  # templates for ORDER BY queries
                {"pattern": "Top 5 <B> ordered by <A> descending",
                 "sql_template": "SELECT <B>, <A> FROM {table_name} ORDER BY <A> DESC LIMIT 5"},

                {"pattern": "Top 5 <B> ordered by <A> ascending",
                 "sql_template": "SELECT <B>, <A> FROM {table_name} ORDER BY <A> ASC LIMIT 5"},

                {"pattern": "All <B> ordered by <A> descending",
                 "sql_template": "SELECT <B>, <A> FROM {table_name} ORDER BY <A> DESC"}
            ],

            "HAVING": [  # templates for HAVING queries
                {"pattern": "Filter <B> with total <A> greater than 100",
                 "sql_template": "SELECT <B>, SUM(<A>) AS total_<A> FROM {table_name} GROUP BY <B> HAVING total_<A> > "
                                 "100"},

                {"pattern": "Filter <B> with average <A> greater than 50",
                 "sql_template": "SELECT <B>, AVG(<A>) AS average_<A> FROM {table_name} GROUP BY <B> HAVING "
                                 "average_<A> > 50"},

                {"pattern": "Filter <B> with count <A> greater than 10",
                 "sql_template": "SELECT <B>, COUNT(<A>) AS count_<A> FROM {table_name} GROUP BY <B> "
                                 "HAVING count_<A> > 10"}
            ],

            "WHERE": [  # templates for WHERE queries
                {"pattern": "Select rows where <A> > 100",
                 "sql_template": "SELECT * FROM {table_name} WHERE <A> > 100"},

                {"pattern": "Select rows where <A> is not null",
                 "sql_template": "SELECT * FROM {table_name} WHERE <A> IS NOT NULL"},

                {"pattern": "Select rows where <B> is null",
                 "sql_template": "SELECT * FROM {table_name} WHERE <B> IS NULL"},

                {"pattern": "Select rows where <A> between <value1> and <value2>",
                 "sql_template": "SELECT * FROM {table_name} WHERE <A> BETWEEN <value1> AND <value2>"},

                {"pattern": "Select rows where <A> like '%<B>%'",
                 "sql_template": "SELECT * FROM {table_name} WHERE <A> LIKE '%<B>%'"},

                {"pattern": "Select rows where <A> >= 100 and <A> <= 200",
                 "sql_template": "SELECT * FROM {table_name} WHERE <A> >= 100 AND <A> <= 200"},

                {"pattern": "Select rows where <A> in ('<val1>', '<val2>', '<val3>')",
                 "sql_template": "SELECT * FROM {table_name} WHERE <A> IN ('<val1>', '<val2>', '<val3>')"}
            ]
        }

        if construct.upper() not in query_templates:  # if construct is not valid, return empty list
            return []

        queries = []  # initialize list for storing queries
        templates = query_templates[construct.upper()]  # get templates for the specified construct
        for quantitative in quantitative_columns:
            for categorical in categorical_columns:
                for template in templates:  # loop through each template
                    if "<A>" not in template["sql_template"] and "<B>" not in template["sql_template"]:
                        # if no placeholders <A> or <B> are present, directly format the query
                        query = template["sql_template"].format(table_name=table_name)
                        natural_language = template["pattern"]
                        queries.append({"description": natural_language, "query": query})
                    else:
                        # replace placeholders <A> and <B> with actual columns
                        query = template["sql_template"].replace("<A>", quantitative).replace("<B>", categorical)
                        query = query.format(table_name=table_name)
                        natural_language = template["pattern"].replace("<A>", quantitative).replace("<B>", categorical)
                        queries.append({"description": natural_language, "query": query})

        unique_queries = {query["query"]: query for query in queries}  # ensure queries are unique
        sampled_queries_construct = random.sample(list(unique_queries.values()), min(3, len(unique_queries)))
        return sampled_queries_construct  # return the sampled queries
