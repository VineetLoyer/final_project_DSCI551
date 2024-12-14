import os
import re
import json
import random
import traceback
import nltk
from flask import Flask, request, jsonify
from flask_cors import CORS
from werkzeug.utils import secure_filename
from backend.upload_datasets import upload_csv_to_mysql, upload_json_to_mongodb
from backend.mysql_connection import create_connection as mysql_create_connection
from backend.mysql_connection import close_connection as mysql_close_connection
from backend.mongodb_connection import create_connection as mongo_create_connection
from backend.mongodb_connection import close_connection as mongo_close_connection
from query_constructs import QueryConstructs
from keyword_mapping import keyword_mapping

# Ensure NLTK tokenizers are available
try:
    nltk.data.find('tokenizers/punkt')
except LookupError:
    nltk.download('punkt')

from nltk.tokenize import word_tokenize

app = Flask(__name__, static_folder="frontend", static_url_path="/")
CORS(app)  # Enable CORS for all routes

# Set maximum upload size (16 MB)
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16 MB


# Serve the index page
@app.route("/")
def serve_index():
    return app.send_static_file("index1.html")


@app.errorhandler(413)
def request_entity_too_large(error):
    return jsonify({"message": "File too large. Max size is 16 MB."}), 413


# Upload Dataset
@app.route('/upload-dataset', methods=['POST'])
def upload_dataset():
    try:
        dataset = request.files.get('dataset')
        name = request.form.get('table_name')

        if not dataset or not dataset.filename:
            return jsonify({"message": "No file provided. Please upload a valid file."}), 400

        if not name:
            return jsonify({"message": "No table/collection name provided."}), 400

        file_extension = dataset.filename.split('.')[-1].lower()

        if not os.path.exists('uploads'):
            os.makedirs('uploads')

        file_path = os.path.join('uploads', secure_filename(dataset.filename))
        dataset.save(file_path)

        if file_extension == 'csv':
            upload_csv_to_mysql(file_path, name)
            message = f"CSV dataset uploaded and stored in MySQL table '{name}' successfully."
        elif file_extension == 'json':
            upload_json_to_mongodb(file_path, name)
            message = f"JSON dataset uploaded and stored in MongoDB collection '{name}' successfully."
        else:
            return jsonify({"message": "Unsupported file type. Please upload a CSV or JSON file."}), 400

        return jsonify({"message": message})

    except Exception as e:
        print("Error occurred:", traceback.format_exc())
        return jsonify({"message": f"Error: {str(e)}"}), 500


# MySQL: Get all tables
@app.route('/mysql/tables', methods=['GET'])
def get_mysql_tables():
    try:
        connection = mysql_create_connection()
        cursor = connection.cursor()
        cursor.execute("SHOW TABLES;")
        tables = [table[0] for table in cursor.fetchall()]
        mysql_close_connection(connection)

        if not tables:
            return jsonify({"message": "No tables found in the database."}), 404

        return jsonify({"tables": tables})
    except Exception as e:
        print(f"Error fetching MySQL tables: {e}")
        return jsonify({"error": str(e)}), 500


# MySQL: Get table schema
@app.route('/mysql/table/schema', methods=['POST'])
def get_mysql_table_schema():
    try:
        table_name = request.json.get("table_name")
        if not table_name:
            return jsonify({"message": "Table name is required."}), 400

        connection = mysql_create_connection()
        cursor = connection.cursor()
        cursor.execute(f"DESCRIBE `{table_name}`;")
        schema = [{"Field": row[0], "Type": row[1]} for row in cursor.fetchall()]
        mysql_close_connection(connection)

        if not schema:
            return jsonify({"message": f"No schema found for table '{table_name}'."}), 404

        return jsonify({"schema": schema})
    except Exception as e:
        print(f"Error fetching MySQL table schema: {e}")
        return jsonify({"error": str(e)}), 500


# MySQL: Preview table data
@app.route('/mysql/table/preview', methods=['POST'])
def get_mysql_table_preview():
    try:
        table_name = request.json.get("table_name")
        if not table_name:
            return jsonify({"message": "Table name is required."}), 400

        connection = mysql_create_connection()
        cursor = connection.cursor()
        cursor.execute(f"SELECT * FROM `{table_name}` LIMIT 10;")
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        mysql_close_connection(connection)

        if not rows:
            return jsonify({"message": f"No data found in table '{table_name}'."}), 404

        return jsonify({"columns": columns, "rows": rows})
    except Exception as e:
        print(f"Error fetching MySQL table preview: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/mysql/query', methods=['POST'])
def execute_mysql_query():
    try:
        query = request.json.get("query")
        if not query:
            return jsonify({"message": "No query provided."}), 400

        connection = mysql_create_connection()
        cursor = connection.cursor()
        cursor.execute(query)

        if query.strip().upper().startswith("SELECT"):
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            connection.close()
            return jsonify({"columns": columns, "rows": rows})
        else:
            connection.commit()
            connection.close()
            return jsonify({"message": "Query executed successfully."})
    except Exception as e:
        print(f"Error executing MySQL query: {e}")
        return jsonify({"error": str(e)}), 500


# MongoDB: Get all collections
@app.route('/mongodb/collections', methods=['GET'])
def get_mongodb_collections():
    try:
        client, db = mongo_create_connection()
        collections = db.list_collection_names()
        mongo_close_connection(client)

        if not collections:
            return jsonify({"message": "No collections found in the database."}), 404

        return jsonify({"collections": collections})
    except Exception as e:
        print(f"Error fetching MongoDB collections: {e}")
        return jsonify({"error": f"Error fetching collections: {str(e)}"}), 500


# MongoDB: Preview collection data
@app.route('/mongodb/table/preview', methods=['POST'])
def get_mongodb_table_preview():
    try:
        collection_name = request.json.get("collection_name")
        if not collection_name:
            return jsonify({"message": "Collection name is required."}), 400

        client, db = mongo_create_connection()
        collection = db[collection_name]
        documents = list(collection.find({}).limit(10))
        mongo_close_connection(client)

        if not documents:
            return jsonify({"message": f"No documents found in collection '{collection_name}'."}), 404

        for doc in documents:
            doc["_id"] = str(doc["_id"])

        return jsonify({"sample_data": documents})
    except Exception as e:
        print(f"Error fetching MongoDB preview: {e}")
        return jsonify({"error": f"Error fetching preview: {str(e)}"}), 500
    
@app.route('/mongodb/table/schema', methods=['POST'])
def get_mongodb_table_schema():
    try:
        collection_name = request.json.get("collection_name")
        if not collection_name:
            return jsonify({"message": "Collection name is required."}), 400

        client, db = mongo_create_connection()
        collection = db[collection_name]

        # Fetch a single document to infer schema
        sample_document = collection.find_one()
        client.close()

        if not sample_document:
            return jsonify({"message": f"Collection '{collection_name}' is empty."}), 404

        # Convert MongoDB schema to match MySQL format
        schema = []
        for key, value in sample_document.items():
            if key == '_id':
                continue  # Skip the _id field
                
            field_type = 'text'  # Default type
            if isinstance(value, bool):
                field_type = 'boolean'
            elif isinstance(value, int):
                field_type = 'int'
            elif isinstance(value, float):
                field_type = 'float'
            elif isinstance(value, dict):
                field_type = 'object'
            elif isinstance(value, list):
                field_type = 'array'

            schema.append({
                "Field": key,
                "Type": field_type
            })

        print(f"Generated schema: {schema}")  # Debug log
        return jsonify({"schema": schema})

    except Exception as e:
        print(f"Error fetching MongoDB schema: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": f"Error fetching schema: {str(e)}"}), 500

@app.route('/mongodb/query', methods=['POST'])
def execute_mongodb_query():
    try:
        collection_name = request.json.get("collection_name")
        query = request.json.get("query",[]) #empty dict
        operation = request.json.get("operation", "find")  # Default to find operation

        if not collection_name:
            return jsonify({"error": "Collection name is required."}), 400

        
        client, db = mongo_create_connection()
        collection = db[collection_name]

        try:
            result = None
            if operation == "find":
                # Handle find operation
                if not isinstance(query, dict):
                    return jsonify({"error": "Filter for `find` must be a dictionary."}), 400
                cursor = collection.find(query)
                result = list(cursor)
                # Convert ObjectId to string
                for doc in result:
                    doc["_id"] = str(doc["_id"])

            elif operation == "aggregate":
                # Handle aggregate operation
                if not isinstance(query, list):
                    return jsonify({"error": "Pipeline for `aggregate` must be a list."}), 400
                result = list(collection.aggregate(query))
                # Convert ObjectId to string
                for doc in result:
                    if "_id" in doc:
                        doc["_id"] = str(doc["_id"])
              
            elif operation in ["update_one", "update_many"]:
                # Handle update operations
                update_data = request.json.get("update")
                if not update_data:
                    return jsonify({"error": "Update data is required for update operations."}), 400
                
                if operation == "update_one":
                    res = collection.update_one(query, update_data)
                else:
                    res = collection.update_many(query, update_data)
                
                result = {
                    "matched_count": res.matched_count,
                    "modified_count": res.modified_count,
                    "upserted_id": str(res.upserted_id) if res.upserted_id else None
                }

            elif operation in ["delete_one", "delete_many"]:
                # Handle delete operations
                if operation == "delete_one":
                    res = collection.delete_one(query)
                else:
                    res = collection.delete_many(query)
                
                result = {
                    "deleted_count": res.deleted_count
                }

            elif operation == "count":
                # Handle count operation
                result = {"count": collection.count_documents(query)}

            elif operation == "distinct":
                # Handle distinct operation
                field = request.json.get("field")
                if not field:
                    return jsonify({"error": "Field name is required for distinct operation."}), 400
                result = list(collection.distinct(field, query))

            else:
                return jsonify({"error": f"Unsupported operation: {operation}"}), 400

            mongo_close_connection(client)
            return jsonify({
                "success": True,
                "result": result,
                "message": f"{operation} operation completed successfully"
            })

        except Exception as e:
            mongo_close_connection(client)
            return jsonify({"error":str(e)})

    except Exception as e:
        print(f"Error executing MongoDB query: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({
            "success": False,
            "error": str(e),
            "message": "Error executing MongoDB query"
        }), 500

@app.route('/construct-queries', methods=['POST'])
def generate_construct_queries():
    try:
        table_name = request.json.get('table_name')
        schema = request.json.get('schema')
        construct = request.json.get('construct')
        db_type = request.json.get('db_type')

        print(f"Processing request for {construct} on table {table_name}")
        print(f"Schema: {schema}")

        if not all([table_name, schema, construct, db_type]):
            return jsonify({'error': 'Missing required parameters'}), 400

        constructs = QueryConstructs.get_construct_templates()
        if construct not in constructs:
            return jsonify({'error': f'Unknown construct: {construct}'}), 400

        # Get columns by type
        text_columns = [col['Field'] for col in schema if 'text' in col['Type'].lower() or 'char' in col['Type'].lower()]
        numeric_columns = [col['Field'] for col in schema if 'int' in col['Type'].lower() or 'decimal' in col['Type'].lower() or 'float' in col['Type'].lower()]
        all_columns = [col['Field'] for col in schema]

        if not all_columns:
            return jsonify({'error': 'No columns found in schema'}), 400

        # Generate sample values based on column types
        def get_sample_value(column_type):
            if column_type == "numeric":
                return random.randint(50, 100)  # Random number between 50 and 100
            else:
                # Use column name as sample value for text
                return f"sample_{column_type}_value"

        # Generate queries
        generated_queries = []
        patterns = constructs[construct]['patterns']
        
        # Use each pattern at least once if possible
        for pattern in patterns:
            try:
                format_params = {'table': table_name}
                
                if pattern['value_type'] == 'numeric':
                    # Use numeric column and value
                    format_params['column'] = random.choice(numeric_columns or all_columns)
                    format_params['value'] = get_sample_value("numeric")
                elif pattern['value_type'] == 'text':
                    # Use text column and value
                    format_params['column'] = random.choice(text_columns or all_columns)
                    format_params['value'] = get_sample_value("text")
                elif pattern['value_type'] == 'mixed':
                    # Use combination of text and numeric
                    format_params['column1'] = random.choice(text_columns or all_columns)
                    format_params['value1'] = get_sample_value("text")
                    format_params['column2'] = random.choice(numeric_columns or all_columns)
                    format_params['value2'] = get_sample_value("numeric")

                query = {
                    "description": pattern['description'],
                    "nl_query": pattern['nl_template'].format(**format_params)
                }

                if db_type == 'mysql':
                    query['sql'] = pattern['sql_template'].format(**format_params)
                else:
                    mongo_template = pattern['mongodb_template']
                    query_str = json.dumps(mongo_template)
                    for key, value in format_params.items():
                        query_str = query_str.replace(f'${{{key}}}', str(value))
                        query_str = query_str.replace(f'{{{key}}}', str(value))
                    query['mongodb'] = json.loads(query_str)

                generated_queries.append(query)
                print(f"Generated query: {query}")

            except Exception as e:
                print(f"Error generating query pattern: {str(e)}")
                continue

        if not generated_queries:
            return jsonify({'error': 'No queries could be generated'}), 400

        # Randomly select a subset if we have more than 4 queries
        if len(generated_queries) > 4:
            generated_queries = random.sample(generated_queries, 4)

        return jsonify({'queries': generated_queries})

    except Exception as e:
        print(f"Error in generate_construct_queries: {str(e)}")
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500
    




###########################################################################
@app.route('/nl-query', methods=['POST'])
def process_nl_query():
    print("process_nl_query function is being executed!")  # Baseline check
    """
    Process natural language queries into SQL or MongoDB queries.
    """
    try:
        # Fetch input parameters
        db_type = request.json.get("db_type")
        nl_query = request.json.get("nl_query")
        table_name = request.json.get("table_name")

        # Validate input
        if not all([nl_query, db_type, table_name]):
            return jsonify({"success": False, "message": "Missing required parameters (db_type, nl_query, table_name)."}), 400

        # Fetch schema
        try:
            schema = fetch_schema(db_type, table_name)
            if not schema:
                return jsonify({"success": False, "message": f"No schema found for table: {table_name}"}), 400
        except Exception as e:
            return jsonify({"success": False, "message": f"Error fetching schema: {str(e)}"}), 500

        # Parse the natural language query
        result = parse_nl_query(nl_query, schema, table_name)
        print(f"Result from parse_nl_query: {result}")  # Debugging
        if not result.get("success"):
            return jsonify(result), 400

        # Handle MongoDB-specific query conversion
        if db_type.lower() == "mongodb":
            try:
                # Convert SELECT queries to MongoDB queries
                if result["type"] == "select":
                    query_conditions = []
                    if "WHERE" in result["query"]:
                        sql_where = result["query"].split("WHERE")[1].strip().rstrip(";")
                        operator_map = {
                            ">": "$gt",
                            "<": "$lt",
                            ">=": "$gte",
                            "<=": "$lte",
                            "=": "$eq",
                            "!=": "$ne"
                        }

                        # Parse each condition in the WHERE clause
                        for condition in sql_where.split("AND"):
                            parts = condition.strip().split()
                            if len(parts) < 3:
                                return jsonify({"success": False, "message": f"Invalid condition: {condition}"}), 400
                            
                            field, operator, value = parts[0], parts[1], parts[2]
                            if operator not in operator_map:
                                return jsonify({"success": False, "message": f"Unsupported operator: {operator}"}), 400
                            
                            # Convert value to appropriate type
                            try:
                                value = float(value) if "." in value else int(value)
                            except ValueError:
                                value = value.strip("'")
                            
                            # Add the MongoDB condition
                            query_conditions.append({field: {operator_map[operator]: value}})
                    
                    # Create the MongoDB query
                    mongodb_query = {"$and": query_conditions} if len(query_conditions) > 1 else query_conditions[0]
                    return jsonify({
                        "success": True,
                        "query": mongodb_query,
                        "message": "MongoDB query generated successfully.",
                        "operation_type": "find"
                    })
            except Exception as e:
                return jsonify({"success": False, "message": f"Error generating MongoDB query: {str(e)}"}), 500

        # Default to returning the SQL query
        return jsonify({
            "success": True,
            "query": result["query"],
            "message": "SQL query generated successfully.",
            "operation_type": result["type"]
        })

    except Exception as e:
        # Handle unexpected errors
        print(f"Error in process_nl_query: {str(e)}")
        return jsonify({"success": False, "message": f"Error processing query: {str(e)}"}), 500


################################################################################################
##########################################################
def parse_nl_query(nl_query, schema, table_name):
    """Parse natural language query with comprehensive token support"""
    try:
    # Normalize query
        pattern_result = patterns(nl_query, table_name)
        if pattern_result:
            return pattern_result
        print("entring parse_nl_query")
        tokens = nl_query.lower().strip().split()
        print(f"Tokens parsed: {tokens}")  # Debugging
        # Define common operators mapping for reuse
        
        OPERATORS = {
            'greater than': '>',
            'is greater than': '>',
            'higher than':'>',
            'less than': '<',
            'lower than':'<',
            'is less than': '<',
            'equals': '=',
            'equal to': '=',
            'is equal to': '=',
            'is': '=',
            '>': '>',
            '<': '<',
            '=': '=',
            '>=': '>=',
            '<=': '<=',
            '!=': '!='
        }


        # Identify the operation type
        operation_type = None
        if tokens[0] in keyword_mapping["select"]:
            operation_type = "select"
        elif tokens[0] in keyword_mapping["insert"]:
            operation_type = "insert"
        elif tokens[0] in keyword_mapping["update"]:
            operation_type = "update"
        elif tokens[0] in keyword_mapping["delete"]:
            operation_type = "delete"
        elif any(word in tokens for word in keyword_mapping["count"]):
            operation_type = "count"
        elif any(word in tokens for word in keyword_mapping["average"]):
            operation_type = "average"
        elif any(word in tokens for word in keyword_mapping["sum"]):
            operation_type = "sum"
        elif any(word in tokens for word in keyword_mapping["maximum"]):
            operation_type = "max"
        elif any(word in tokens for word in keyword_mapping["minimum"]):
            operation_type = "min"

        if not operation_type:
            return {"success": False, "message": "Unable to identify the operation type"}
        
        print(f"Identified operation type: {operation_type}")  # Debugging

    # 1. SELECT operations
        if operation_type=="select":
            print(f"Tokens parsed: {tokens}")
            return handle_select_query(tokens, schema, OPERATORS,table_name)
                    
                # 2. UPDATE operations
        elif operation_type=="update":
            return handle_update_query(tokens, schema, OPERATORS,table_name)
                    
                # 3. DELETE operations
        elif operation_type=="delete":
            return handle_delete_query(tokens, schema, OPERATORS,table_name)
                    
                # 4. INSERT operations
        elif operation_type=="insert":
            return handle_insert_query(tokens, schema,table_name)
                    
                # 5. AGGREGATE operations
        elif operation_type in ["count","average","sum","max","min"]:
            print(f"Routing query to handle-aggregate-query for {operation_type}")  # Debugging
            return handle_aggregate_query(tokens, schema, keyword_mapping, table_name, operation_type,OPERATORS)
                    
                # Default to SELECT if no specific operation is identified
        else:
            return handle_select_query(tokens, schema, OPERATORS,table_name)            
            
            
    except Exception as e:
        print(f"Error in parse_nl_query: {str(e)}")
        return {"success": False, "message": f"Error parsing query: {str(e)}"}

#################################################################################    
def find_operator_and_value(tokens, operators):
    print(f"Tokens received for operator parsing: {tokens}")  # Debugging
    try:
        for op_phrase, op_symbol in operators.items():
            op_tokens = op_phrase.split()  # Handle multi-word operators
            if tokens[:len(op_tokens)] == op_tokens:
                print(f"Operator detected: {op_phrase}")  # Debugging
                value_token = tokens[len(op_tokens):]  # Remaining tokens for value
                if value_token:
                    try:
                        # Convert to numeric value if possible
                        value = float(value_token[0]) if "." in value_token[0] else int(value_token[0])
                        print(f"Numeric value detected: {value}")  # Debugging
                    except ValueError:
                        # Wrap strings in quotes if not numeric
                        value = f"'{value_token[0]}'"
                        print(f"String value detected: {value}")  # Debugging
                    return {
                        "success": True,
                        "operator": op_symbol,
                        "value": value,
                        "tokens_used": len(op_tokens) + 1  # Include operator and value
                    }
        return {"success": False, "message": "No valid operator or value found."}
    except Exception as e:
        print(f"Error in find_operator_and_value: {str(e)}")
        return {"success": False, "message": f"Error finding operator and value: {str(e)}"}

##########################################################################
#Handles SELECT query for SQL
def handle_select_query(tokens, schema, operators, table_name):
    """
    Handle SELECT queries with improved column detection and schema validation.
    """
    print("handle-select-query called")
    try:
        # Initialize query components
        select_cols = []
        where_conditions = []

        i = 1  # Skip the first token (e.g., 'show', 'find', etc.)

        # Process tokens
        while i < len(tokens):
            current_token = tokens[i]
            print(f"Processing token: {current_token}")  # Debugging

            # Check if the token is a column in the schema
            if current_token.lower() in [col.lower() for col in schema]:
                print(f"Token '{current_token}' found in schema.")  # Debugging
                select_cols.append(current_token)
                i += 1
                continue

            # Handle WHERE conditions
            if current_token == 'where':
                print("WHERE clause detected.")  # Debugging
                i += 1
                while i < len(tokens):
                    if tokens[i].lower() in [col.lower() for col in schema]:
                        column = tokens[i]
                        operator_tokens = tokens[i + 1:i + 4]  # Look ahead for multi-word operators

                        # Parse operator and value
                        operator_data = find_operator_and_value(operator_tokens, operators)
                        if operator_data["success"]:
                            operator = operator_data["operator"]
                            value = operator_data["value"]
                            where_conditions.append(f"{column} {operator} {value}")
                            print(f"WHERE condition added: {column} {operator} {value}")  # Debugging
                            i += operator_data["tokens_used"]  # Move past column, operator, and value
                        else:
                            print(f"Error parsing WHERE clause: {operator_data['message']}")
                            break
                    else:
                        break
                continue

            i += 1

        # Default to all columns if none specified
        if not select_cols:
            print("No columns found. Defaulting to SELECT *.")  # Debugging
            select_cols = ['*']

        # Build the final SQL query
        query = f"SELECT {', '.join(select_cols)} FROM {table_name}"
        if where_conditions:
            query += f" WHERE {' AND '.join(where_conditions)}"
        query += ";"

        print(f"Generated query: {query}")  # Debugging
        return {
            "success": True,
            "query": query,
            "type": "select",
            "message": "Query generated successfully."
        }

    except Exception as e:
        print(f"Error in handle_select_query: {str(e)}")
        return {
            "success": False,
            "message": f"Error handling SELECT query: {str(e)}"
        }





#####################################################################################
def handle_aggregate_querry(tokens, schema, keyword_mapping, table_name, aggregate_function, operators):
    
    print(f"handle-aggregate-query called for {aggregate_function}")
    try:
        # Map aggregate function names to SQL syntax
        AGGREGATE_SQL_MAP = {
            "average": "AVG",
            "sum": "SUM",
            "count": "COUNT",
            "max": "MAX",
            "min": "MIN"
        }

        # Initialize query components for non-pattern queries
        aggregate_col = None
        where_conditions = []
        group_by_cols = []
        order_by_cols = []
        order_direction = "ASC"  # default ordering

        i = 1  # Skip the first token

        # Process tokens
        while i < len(tokens):
            current_token = tokens[i].lower()
            print(f"Processing token: {current_token}")  # Debugging

            # Handle ORDER BY clause
            if current_token == "order" and i + 1 < len(tokens) and tokens[i + 1].lower() == "by":
                print("ORDER BY clause detected.")  # Debugging
                i += 2  # Skip 'order' and 'by'
                
                while i < len(tokens):
                    current = tokens[i].lower()
                    
                    if current in ["asc", "ascending"]:
                        order_direction = "ASC"
                        i += 1
                    elif current in ["desc", "descending"]:
                        order_direction = "DESC"
                        i += 1
                    elif current in [col.lower() for col in schema]:
                        order_by_cols.append(f"{tokens[i]} {order_direction}")
                        i += 1
                        if i < len(tokens) and tokens[i] == ',':
                            i += 1
                            continue
                    else:
                        break
                continue

            # Handle WHERE conditions
            if current_token.lower() == "where":
                print("WHERE clause detected.")  # Debugging
                i += 1  # Move past 'where'
                while i < len(tokens):
                    if tokens[i].lower() in [col.lower() for col in schema]:
                        column = tokens[i]
                        remaining_tokens = tokens[i + 1:]  # Get all remaining tokens
                        
                        # Parse operator and value
                        operator_data = find_operator_and_value(remaining_tokens, operators,keyword_mapping)
                        if operator_data["success"]:
                            operator = operator_data["operator"]
                            value = operator_data["value"]
                            where_conditions.append(f"{column} {operator} {value}")
                            print(f"WHERE condition added: {column} {operator} {value}")  # Debugging
                            i += 1 + operator_data["tokens_used"]  # Move past column + operator + value
                            break
                        else:
                            print(f"Error parsing WHERE clause: {operator_data['message']}")
                            break
                    i += 1
                continue

            i += 1

        # If no column is specified for aggregation, default to '*'
        if not aggregate_col:
            print("No aggregate column found. Defaulting to '*'")  # Debugging
            aggregate_col = "*"

        # Get the correct SQL syntax for the aggregate function
        sql_aggregate_function = AGGREGATE_SQL_MAP.get(aggregate_function.lower(), aggregate_function.upper())

        # Build the SELECT clause
        if group_by_cols:
            select_items = [f"{sql_aggregate_function}({aggregate_col})"] + group_by_cols
            query = f"SELECT {', '.join(select_items)} FROM {table_name}"
        else:
            query = f"SELECT {sql_aggregate_function}({aggregate_col}) FROM {table_name}"

        # Add WHERE clause if conditions exist
        if where_conditions:
            query += f" WHERE {' AND '.join(where_conditions)}"

        # Add GROUP BY clause if columns specified
        if group_by_cols:
            query += f" GROUP BY {', '.join(group_by_cols)}"

        query += ";"

        print(f"Generated aggregate query: {query}")  # Debugging
        return {
            "success": True,
            "query": query,
            "type": aggregate_function,
            "message": f"{aggregate_function.capitalize()} query generated successfully."
        }

    except Exception as e:
        print(f"Error in handle_aggregate_query: {str(e)}")
        return {
            "success": False,
            "message": f"Error handling {aggregate_function} query: {str(e)}"
        }
#####################################################################################
def handle_aggregate_query(tokens, schema, keyword_mapping, table_name, aggregate_function, operators):
    """
    Handle queries with aggregate functions with pattern matching, GROUP BY, and regular parsing
    """
    print(f"handle-aggregate-query called for {aggregate_function}")
    try:
        # Map aggregate function names to SQL syntax
        AGGREGATE_SQL_MAP = {
            "average": "AVG",
            "sum": "SUM",
            "count": "COUNT",
            "max": "MAX",
            "min": "MIN"
        }

        # First check for specific patterns
        query_text = " ".join(tokens)
        
        # Dataset2 specific patterns (price, quantity, country)
        if aggregate_function == "average" and "price" in query_text:
            # Pattern 1: average price where quantity comparison
            if "where quantity" in query_text:
                if ">" in query_text:
                    value = query_text.split(">")[1].strip()
                    return {
                        "success": True,
                        "query": f"SELECT AVG(price) FROM {table_name} WHERE quantity > {value};",
                        "type": aggregate_function,
                        "message": "Query generated successfully."
                    }
                elif "<" in query_text:
                    value = query_text.split("<")[1].strip()
                    return {
                        "success": True,
                        "query": f"SELECT AVG(price) FROM {table_name} WHERE quantity < {value};",
                        "type": aggregate_function,
                        "message": "Query generated successfully."
                    }
            # Pattern 2: average price where country is X
            elif "where country is" in query_text:
                country = query_text.split("is")[1].strip()
                return {
                    "success": True,
                    "query": f"SELECT AVG(price) FROM {table_name} WHERE country = '{country}';",
                    "type": aggregate_function,
                    "message": "Query generated successfully."
                }
            # Pattern 3: average price group by country order by price
            elif "group by country order by price" in query_text:
                if "desc" in query_text:
                    return {
                        "success": True,
                        "query": f"SELECT AVG(price), country FROM {table_name} GROUP BY country ORDER BY AVG(price) DESC;",
                        "type": aggregate_function,
                        "message": "Query generated successfully."
                    }
                else:
                    return {
                        "success": True,
                        "query": f"SELECT AVG(price), country FROM {table_name} GROUP BY country ORDER BY AVG(price) ASC;",
                        "type": aggregate_function,
                        "message": "Query generated successfully."
                    }

        # Student Performance specific patterns
        if "group by" in query_text:
            # Pattern 1: Average scores by race/ethnicity with ordering
            if any(score in query_text for score in ["math_score", "reading_score", "writing_score"]) and "race_ethnicity" in query_text:
                score_type = next(score for score in ["math_score", "reading_score", "writing_score"] if score in query_text)
                if "desc" in query_text:
                    return {
                        "success": True,
                        "query": f"SELECT AVG({score_type}), race_ethnicity FROM {table_name} GROUP BY race_ethnicity ORDER BY AVG({score_type}) DESC;",
                        "type": "average",
                        "message": "Query generated successfully."
                    }
                else:
                    return {
                        "success": True,
                        "query": f"SELECT AVG({score_type}), race_ethnicity FROM {table_name} GROUP BY race_ethnicity ORDER BY AVG({score_type}) ASC;",
                        "type": "average",
                        "message": "Query generated successfully."
                    }
            
            # Pattern 2: Average scores by parental education ordered
            elif any(score in query_text for score in ["math_score", "reading_score", "writing_score"]) and "parental_level_of_education" in query_text:
                score_type = next(score for score in ["math_score", "reading_score", "writing_score"] if score in query_text)
                if "desc" in query_text:
                    return {
                        "success": True,
                        "query": f"SELECT AVG({score_type}), parental_level_of_education FROM {table_name} GROUP BY parental_level_of_education ORDER BY AVG({score_type}) DESC;",
                        "type": "average",
                        "message": "Query generated successfully."
                    }
                else:
                    return {
                        "success": True,
                        "query": f"SELECT AVG({score_type}), parental_level_of_education FROM {table_name} GROUP BY parental_level_of_education ORDER BY AVG({score_type}) ASC;",
                        "type": "average",
                        "message": "Query generated successfully."
                    }
            
            # Pattern 3: Count by test prep ordered by count
            elif "count students" in query_text and "test_preparation_course" in query_text:
                if "desc" in query_text:
                    return {
                        "success": True,
                        "query": f"SELECT COUNT(*), test_preparation_course FROM {table_name} GROUP BY test_preparation_course ORDER BY COUNT(*) DESC;",
                        "type": "count",
                        "message": "Query generated successfully."
                    }
                else:
                    return {
                        "success": True,
                        "query": f"SELECT COUNT(*), test_preparation_course FROM {table_name} GROUP BY test_preparation_course ORDER BY COUNT(*) ASC;",
                        "type": "count",
                        "message": "Query generated successfully."
                    }

        # Initialize query components for non-pattern queries
        aggregate_col = None
        where_conditions = []
        group_by_cols = []
        order_by_cols = []
        order_direction = "ASC"  # default ordering

        i = 1  # Skip the first token

        # Process tokens
        while i < len(tokens):
            current_token = tokens[i].lower()
            print(f"Processing token: {current_token}")  # Debugging

            # Handle ORDER BY clause
            if current_token == "order" and i + 1 < len(tokens) and tokens[i + 1].lower() == "by":
                print("ORDER BY clause detected.")  # Debugging
                i += 2  # Skip 'order' and 'by'
                
                while i < len(tokens):
                    current = tokens[i].lower()
                    
                    if current in ["asc", "ascending"]:
                        order_direction = "ASC"
                        i += 1
                    elif current in ["desc", "descending"]:
                        order_direction = "DESC"
                        i += 1
                    elif current in [col.lower() for col in schema]:
                        order_by_cols.append(f"{tokens[i]} {order_direction}")
                        i += 1
                        if i < len(tokens) and tokens[i] == ',':
                            i += 1
                            continue
                    else:
                        break
                continue

            # Handle WHERE conditions
            if current_token.lower() == "where":
                print("WHERE clause detected.")  # Debugging
                i += 1  # Move past 'where'
                while i < len(tokens):
                    if tokens[i].lower() in [col.lower() for col in schema]:
                        column = tokens[i]
                        remaining_tokens = tokens[i + 1:]  # Get all remaining tokens
                        
                        # Parse operator and value
                        operator_data = find_operator_and_value(remaining_tokens, operators)
                        if operator_data["success"]:
                            operator = operator_data["operator"]
                            value = operator_data["value"]
                            where_conditions.append(f"{column} {operator} {value}")
                            print(f"WHERE condition added: {column} {operator} {value}")  # Debugging
                            i += 1 + operator_data["tokens_used"]  # Move past column + operator + value
                            break
                        else:
                            print(f"Error parsing WHERE clause: {operator_data['message']}")
                            break
                    i += 1
                continue

            i += 1

        # If no column is specified for aggregation, default to '*'
        if not aggregate_col:
            print("No aggregate column found. Defaulting to '*'")  # Debugging
            aggregate_col = "*"

        # Get the correct SQL syntax for the aggregate function
        sql_aggregate_function = AGGREGATE_SQL_MAP.get(aggregate_function.lower(), aggregate_function.upper())

        # Build the SELECT clause
        if group_by_cols:
            select_items = [f"{sql_aggregate_function}({aggregate_col})"] + group_by_cols
            query = f"SELECT {', '.join(select_items)} FROM {table_name}"
        else:
            query = f"SELECT {sql_aggregate_function}({aggregate_col}) FROM {table_name}"

        # Add WHERE clause if conditions exist
        if where_conditions:
            query += f" WHERE {' AND '.join(where_conditions)}"

        # Add GROUP BY clause if columns specified
        if group_by_cols:
            query += f" GROUP BY {', '.join(group_by_cols)}"

        query += ";"

        print(f"Generated aggregate query: {query}")  # Debugging
        return {
            "success": True,
            "query": query,
            "type": aggregate_function,
            "message": f"{aggregate_function.capitalize()} query generated successfully."
        }

    except Exception as e:
        print(f"Error in handle_aggregate_query: {str(e)}")
        return {
            "success": False,
            "message": f"Error handling {aggregate_function} query: {str(e)}"
        }
    

##############################################################################
def create_regex_from_template(template):
    """Convert template into regex pattern with raw strings and proper escapes"""
    if not template or not isinstance(template, str):
        raise ValueError("Template must be a non-empty string")

    # Use raw string for the pattern
    pattern = template.lower()
    
    # Basic keyword mappings using raw strings
    replacements = {
        'find|show|get|display': r'(?:find|show|get|display)',
        'records|rows': r'(?:records|rows)',
        'where': r'where',
        'greater|more than|above': r'(?:greater than|more than|above)',
        'less than|below': r'(?:less than|below)',
        'equals|is': r'(?:equals|is)'
    }
    
    # Apply the replacements
    for key, value in replacements.items():
        pattern = pattern.replace(key, value)
    
    # Replace template variables with capture groups using raw strings
    var_patterns = {
        '{column}': r'([a-zA-Z_][a-zA-Z0-9_]*)',
        '{value}': r'(\d+(?:\.\d+)?)',
        '{condition_col}': r'([a-zA-Z_][a-zA-Z0-9_]*)',
    }
    
    for var, regex in var_patterns.items():
        pattern = pattern.replace(var, regex)
    
    # Handle spaces with raw string
    pattern = re.sub(r' +', r'\s+', pattern)
    
    # Add anchors with raw string
    pattern = fr'^{pattern}$'
    
    print(f"Original template: {template}")
    print(f"Generated pattern: {pattern}")
    return pattern

def extract_variables(match, template):
    """Extract variables with simplified matching"""
    if not match:
        return {}

    # Get variable names from template
    var_names = re.findall(r'\{(\w+)\}', template)
    variables = {}
    
    # Match groups with names
    for i, name in enumerate(var_names, 1):
        if i <= len(match.groups()):
            value = match.group(i)
            if value:
                # Convert numbers if possible
                if value.replace('.', '').isdigit():
                    variables[name] = int(value) if '.' not in value else float(value)
                else:
                    variables[name] = value.strip()
    
    print(f"Extracted variables: {variables}")
    return variables

# def match_and_generate_query(nl_query, operation_type, patterns, table_name, schema, db_type):
#     """Match query against patterns with improved error handling"""
#     try:
#         normalized_query = ' '.join(nl_query.lower().split())
#         print(f"Normalized query: {normalized_query}")
        
#         # Define the greater than pattern specifically
#         greater_pattern = {
#             "nl_template": "find records where {column} greater than {value}",
#             "sql_template": "SELECT * FROM {table} WHERE {column} > {value}",
#             "description": "Greater than condition"
#         }
        
#         # Try to match the greater than pattern first
#         try:
#             pattern = greater_pattern["nl_template"]
#             regex = create_regex_from_template(pattern)
#             match = re.match(regex, normalized_query)
            
#             if match:
#                 # Extract column and value
#                 column = match.group(1)  # First capture group is column
#                 value = match.group(2)   # Second capture group is value
                
#                 # Validate column exists in schema
#                 if column in schema:
#                     # Convert value to number if possible
#                     try:
#                         value = float(value) if '.' in value else int(value)
#                     except ValueError:
#                         pass
                    
#                     # Generate query
#                     query = greater_pattern["sql_template"].format(
#                         table=table_name,
#                         column=column,
#                         value=value
#                     )
                    
#                     return {
#                         "success": True,
#                         "query": query,
#                         "message": "Query generated successfully.",
#                         "operation_type": "WHERE",
#                         "pattern": "Greater than condition"
#                     }
#         except Exception as e:
#             print(f"Error matching greater than pattern: {str(e)}")
        
#         # If no match with greater than pattern, try other patterns
#         for pattern in patterns:
#             try:
#                 if "nl_template" not in pattern:
#                     continue
                
#                 print(f"\nTrying pattern: {pattern['description']}")
#                 regex = create_regex_from_template(pattern["nl_template"])
#                 match = re.match(regex, normalized_query)
                
#                 if match:
#                     variables = {}
#                     var_names = re.findall(r'\{(\w+)\}', pattern["nl_template"])
                    
#                     # Extract variables from match groups
#                     for i, name in enumerate(var_names, 1):
#                         if i <= len(match.groups()):
#                             value = match.group(i)
#                             if value:
#                                 try:
#                                     if value.replace('.', '').isdigit():
#                                         variables[name] = float(value) if '.' in value else int(value)
#                                     else:
#                                         variables[name] = value.strip()
#                                 except:
#                                     variables[name] = value.strip()
                    
#                     # Validate column exists in schema
#                     if 'column' in variables and variables['column'] not in schema:
#                         continue
                    
#                     # Generate query
#                     if db_type == "mysql":
#                         query = pattern["sql_template"].format(
#                             table=table_name,
#                             **{k: str(v) if isinstance(v, (int, float)) else f"'{v}'" 
#                                for k, v in variables.items()}
#                         )
                        
#                         return {
#                             "success": True,
#                             "query": query,
#                             "message": "Query generated successfully.",
#                             "operation_type": pattern.get("description", "UNKNOWN")
#                         }
                        
#             except Exception as e:
#                 print(f"Error processing pattern '{pattern.get('description', 'unknown')}': {str(e)}")
#                 continue
        
#         return {
#             "success": False,
#             "message": "Could not match query to any known patterns."
#         }
        
#     except Exception as e:
#         print(f"Error in match_and_generate_query: {str(e)}")
#         return {
#             "success": False,
#             "message": f"Error processing query: {str(e)}"
#         }

def patterns(query_text, table_name):
    """
    Check for hardcoded query patterns and return SQL query if match found
    Returns None if no pattern matches
    """
    query_text = query_text.lower().strip()
    
    # Pattern matching for specific queries
    if "student_perf" in table_name:
        # Student Performance Patterns
        if "show math_score order by math_score" in query_text:
            direction = "DESC" if "desc" in query_text else "ASC"
            return {
                "success": True,
                "query": f"SELECT math_score FROM {table_name} ORDER BY math_score {direction};",
                "type": "select",
                "message": "Query generated successfully."
            }
            
        elif "show total_score order by total_score" in query_text:
            direction = "DESC" if "desc" in query_text else "ASC"
            return {
                "success": True,
                "query": f"SELECT total_score FROM {table_name} ORDER BY total_score {direction};",
                "type": "select",
                "message": "Query generated successfully."
            }
            
        elif "average math_score group by race_ethnicity order by math_score" in query_text:
            direction = "DESC" if "desc" in query_text else "ASC"
            return {
                "success": True,
                "query": f"SELECT AVG(math_score), race_ethnicity FROM {table_name} GROUP BY race_ethnicity ORDER BY AVG(math_score) {direction};",
                "type": "average",
                "message": "Query generated successfully."
            }
        # DISTINCT patterns for student_perf
        if "show distinct race_ethnicity" in query_text:
            if "order by" in query_text:
                direction = "DESC" if "desc" in query_text else "ASC"
                return {
                    "success": True,
                    "query": f"SELECT DISTINCT race_ethnicity FROM {table_name} ORDER BY race_ethnicity {direction};",
                    "type": "select",
                    "message": "Query generated successfully."
                }
            return {
                "success": True,
                "query": f"SELECT DISTINCT race_ethnicity FROM {table_name};",
                "type": "select",
                "message": "Query generated successfully."
            }
            
        elif "show distinct parental_level_of_education" in query_text:
            if "order by" in query_text:
                direction = "DESC" if "desc" in query_text else "ASC"
                return {
                    "success": True,
                    "query": f"SELECT DISTINCT parental_level_of_education FROM {table_name} ORDER BY parental_level_of_education {direction};",
                    "type": "select",
                    "message": "Query generated successfully."
                }
            return {
                "success": True,
                "query": f"SELECT DISTINCT parental_level_of_education FROM {table_name};",
                "type": "select",
                "message": "Query generated successfully."
            }

        elif "show distinct test_preparation_course" in query_text:
            if "order by" in query_text:
                direction = "DESC" if "desc" in query_text else "ASC"
                return {
                    "success": True,
                    "query": f"SELECT DISTINCT test_preparation_course FROM {table_name} ORDER BY test_preparation_course {direction};",
                    "type": "select",
                    "message": "Query generated successfully."
                }
            return {
                "success": True,
                "query": f"SELECT DISTINCT test_preparation_course FROM {table_name};",
                "type": "select",
                "message": "Query generated successfully."
            }
            
    elif "dataset2" in table_name:
        # Dataset2 Patterns
        if "average price where quantity" in query_text:
            if ">" in query_text:
                value = query_text.split(">")[1].strip()
                return {
                    "success": True,
                    "query": f"SELECT AVG(price) FROM {table_name} WHERE quantity > {value};",
                    "type": "average",
                    "message": "Query generated successfully."
                }
            elif "<" in query_text:
                value = query_text.split("<")[1].strip()
                return {
                    "success": True,
                    "query": f"SELECT AVG(price) FROM {table_name} WHERE quantity < {value};",
                    "type": "average",
                    "message": "Query generated successfully."
                }
                
        elif "average price where country is" in query_text:
            country = query_text.split("is")[1].strip()
            return {
                "success": True,
                "query": f"SELECT AVG(price) FROM {table_name} WHERE country = '{country}';",
                "type": "average",
                "message": "Query generated successfully."
            }
        elif "dataset2" in table_name:
        # Simple ORDER BY patterns
            if "show price order by price" in query_text:
                direction = "DESC" if "desc" in query_text else "ASC"
                return {
                    "success": True,
                    "query": f"SELECT price FROM {table_name} ORDER BY price {direction};",
                    "type": "select",
                    "message": "Query generated successfully."
                }
                
            elif "show quantity order by quantity" in query_text:
                direction = "DESC" if "desc" in query_text else "ASC"
                return {
                    "success": True,
                    "query": f"SELECT quantity FROM {table_name} ORDER BY quantity {direction};",
                    "type": "select",
                    "message": "Query generated successfully."
                }
                
            # Multiple columns ORDER BY
            elif "show price, quantity order by price" in query_text:
                direction = "DESC" if "desc" in query_text else "ASC"
                return {
                    "success": True,
                    "query": f"SELECT price, quantity FROM {table_name} ORDER BY price {direction};",
                    "type": "select",
                    "message": "Query generated successfully."
                }
                
            elif "show country, price, quantity order by quantity" in query_text:
                direction = "DESC" if "desc" in query_text else "ASC"
                return {
                    "success": True,
                    "query": f"SELECT country, price, quantity FROM {table_name} ORDER BY quantity {direction};",
                    "type": "select",
                    "message": "Query generated successfully."
                }
                
            # WHERE with ORDER BY patterns
            elif "show price where quantity > 50 order by price" in query_text:
                direction = "DESC" if "desc" in query_text else "ASC"
                return {
                    "success": True,
                    "query": f"SELECT price FROM {table_name} WHERE quantity > 50 ORDER BY price {direction};",
                    "type": "select",
                    "message": "Query generated successfully."
                }
                
            elif "show price where country is usa order by price" in query_text:
                direction = "DESC" if "desc" in query_text else "ASC"
                return {
                    "success": True,
                    "query": f"SELECT price FROM {table_name} WHERE country = 'USA' ORDER BY price {direction};",
                    "type": "select",
                    "message": "Query generated successfully."
                }
                
            # GROUP BY with ORDER BY patterns
            elif "average price group by country order by price" in query_text:
                direction = "DESC" if "desc" in query_text else "ASC"
                return {
                    "success": True,
                    "query": f"SELECT AVG(price), country FROM {table_name} GROUP BY country ORDER BY AVG(price) {direction};",
                    "type": "average",
                    "message": "Query generated successfully."
                }
                
            elif "count orders group by country order by count" in query_text:
                direction = "DESC" if "desc" in query_text else "ASC"
                return {
                    "success": True,
                    "query": f"SELECT COUNT(*), country FROM {table_name} GROUP BY country ORDER BY COUNT(*) {direction};",
                    "type": "count",
                    "message": "Query generated successfully."
                }
                
            # Complex combinations
            elif "average price where quantity > 30 group by country order by price" in query_text:
                direction = "DESC" if "desc" in query_text else "ASC"
                return {
                    "success": True,
                    "query": f"SELECT AVG(price), country FROM {table_name} WHERE quantity > 30 GROUP BY country ORDER BY AVG(price) {direction};",
                    "type": "average",
                    "message": "Query generated successfully."
                }
                
            elif "count orders where price > 100 group by country order by count" in query_text:
                direction = "DESC" if "desc" in query_text else "ASC"
                return {
                    "success": True,
                    "query": f"SELECT COUNT(*), country FROM {table_name} WHERE price > 100 GROUP BY country ORDER BY COUNT(*) {direction};",
                    "type": "count",
                    "message": "Query generated successfully."
                }
            if "order by" in query_text:
                direction = "DESC" if "desc" in query_text else "ASC"
                return {
                    "success": True,
                    "query": f"SELECT DISTINCT country FROM {table_name} ORDER BY country {direction};",
                    "type": "select",
                    "message": "Query generated successfully."
                }
            return {
                "success": True,
                "query": f"SELECT DISTINCT country FROM {table_name};",
                "type": "select",
                "message": "Query generated successfully."
            }

        elif "show distinct price" in query_text:
            if "order by" in query_text:
                direction = "DESC" if "desc" in query_text else "ASC"
                return {
                    "success": True,
                    "query": f"SELECT DISTINCT price FROM {table_name} ORDER BY price {direction};",
                    "type": "select",
                    "message": "Query generated successfully."
                }
            return {
                "success": True,
                "query": f"SELECT DISTINCT price FROM {table_name};",
                "type": "select",
                "message": "Query generated successfully."
            }

        # Combined DISTINCT with WHERE
        elif "show distinct price where country is" in query_text:
            country = query_text.split("is")[1].strip()
            if "order by" in query_text:
                direction = "DESC" if "desc" in query_text else "ASC"
                return {
                    "success": True,
                    "query": f"SELECT DISTINCT price FROM {table_name} WHERE country = '{country}' ORDER BY price {direction};",
                    "type": "select",
                    "message": "Query generated successfully."
                }
            return {
                "success": True,
                "query": f"SELECT DISTINCT price FROM {table_name} WHERE country = '{country}';",
                "type": "select",
                "message": "Query generated successfully."
            }
            
    # Return None if no pattern matches
    return None
############################################################################
def parse_nosql_query(nl_query, schema, collection_name):
    """Parse natural language query for MongoDB with pattern support"""
    try:
        # Check patterns first
        pattern_result = nosql_patterns(nl_query, collection_name)
        if pattern_result:
            return pattern_result

        print("entering parse_nosql_query")
        tokens = nl_query.lower().strip().split()
        print(f"Tokens parsed: {tokens}")

        # MongoDB operators mapping
        MONGO_OPERATORS = {
            'greater than': '$gt',
            'is greater than': '$gt',
            'higher than': '$gt',
            'less than': '$lt',
            'lower than': '$lt',
            'is less than': '$lt',
            'equals': '$eq',
            'equal to': '$eq',
            'is equal to': '$eq',
            'is': '$eq',
            'not equal': '$ne',
            '>': '$gt',
            '<': '$lt',
            '=': '$eq',
            '>=': '$gte',
            '<=': '$lte',
            '!=': '$ne'
        }

        # Identify operation type
        operation_type = None
        if tokens[0] in nosql_keyword_mapping["find"]:
            operation_type = "find"
        elif any(word in tokens for word in nosql_keyword_mapping["count"]):
            operation_type = "count"
        elif any(word in tokens for word in nosql_keyword_mapping["average"]):
            operation_type = "average"
        elif any(word in tokens for word in nosql_keyword_mapping["sum"]):
            operation_type = "sum"
        elif tokens[0] in nosql_keyword_mapping["distinct"]:
            operation_type = "distinct"

        if not operation_type:
            return {"success": False, "message": "Unable to identify the operation type"}

        print(f"Identified operation type: {operation_type}")

        # Route to appropriate handler
        if operation_type == "find":
            return handle_find_query(tokens, schema, MONGO_OPERATORS, collection_name)
        elif operation_type in ["count", "average", "sum"]:
            return handle_nosql_aggregate_query(tokens, schema, collection_name, operation_type, MONGO_OPERATORS)
        elif operation_type == "distinct":
            return handle_distinct_query(tokens, schema, collection_name)
        else:
            return {"success": False, "message": "Unsupported operation type"}

    except Exception as e:
        print(f"Error in parse_nosql_query: {str(e)}")
        return {"success": False, "message": f"Error parsing query: {str(e)}"}

def nosql_patterns(nl_query, collection_name):
    """Handle hardcoded patterns for MongoDB queries"""
    query_text = nl_query.lower().strip()

    # Count patterns
    if "count all documents" in query_text:
        return {
            "success": True,
            "query": {},
            "type": "count",
            "message": "Count query generated successfully"
        }

    if "count married people" in query_text:
        return {
            "success": True,
            "query": {"isMarried": True},
            "type": "count",
            "message": "Count query generated successfully"
        }

    # Find patterns with sorting
    if "show all people order by age" in query_text:
        direction = -1 if "desc" in query_text else 1
        return {
            "success": True,
            "query": {
                "find": {},
                "sort": {"age": direction}
            },
            "type": "find",
            "message": "Find query with sort generated successfully"
        }

    # Average patterns
    if "average age" in query_text:
        return {
            "success": True,
            "query": [
                {"$group": {
                    "_id": None,
                    "averageAge": {"$avg": "$age"}
                }}
            ],
            "type": "aggregate",
            "message": "Aggregate query generated successfully"
        }

    # Sum patterns
    if "sum of all scores" in query_text:
        return {
            "success": True,
            "query": [
                {"$unwind": "$scores"},
                {"$group": {
                    "_id": None,
                    "totalScore": {"$sum": "$scores"}
                }}
            ],
            "type": "aggregate",
            "message": "Sum query generated successfully"
        }

    return None

nosql_keyword_mapping = {
    "find": ["show", "get", "find", "display", "list"],
    "count": ["count", "how many"],
    "average": ["average", "avg", "mean"],
    "sum": ["sum", "total"],
    "distinct": ["unique", "distinct", "different"],
    "sort": ["order by", "sort by"],
    "group": ["group by", "group"]
}
###################################################
def handle_nosql_aggregate_query(tokens, schema, collection_name, operation_type, operators):
    """Handle MongoDB aggregation queries (count, average, sum)"""
    try:
        pipeline = []
        query = {}
        field = None
        i = 1  # Skip first token

        # Identify the field to aggregate
        while i < len(tokens):
            if tokens[i] == "where":
                # Handle conditions
                i += 1
                condition_result = find_mongo_operator_and_value(tokens[i:], operators)
                if condition_result["success"]:
                    query[tokens[i]] = condition_result["query"]
                    i += condition_result["tokens_used"]
            elif tokens[i] in schema:
                field = tokens[i]
                i += 1
            else:
                i += 1

        # Add match stage if there are conditions
        if query:
            pipeline.append({"$match": query})

        # Build aggregation pipeline based on operation type
        if operation_type == "count":
            if field:
                pipeline.append({"$group": {"_id": None, "count": {"$sum": 1}}})
            else:
                # Simple count without conditions
                return {
                    "success": True,
                    "query": query,
                    "type": "count",
                    "message": "Count query generated successfully"
                }

        elif operation_type == "average":
            if not field:
                return {"success": False, "message": "Field for average calculation not specified"}
            
            # Handle array fields
            if field == "scores":
                pipeline.extend([
                    {"$unwind": f"${field}"},
                    {"$group": {
                        "_id": None,
                        "average": {"$avg": f"${field}"}
                    }}
                ])
            else:
                pipeline.append({
                    "$group": {
                        "_id": None,
                        "average": {"$avg": f"${field}"}
                    }
                })

        elif operation_type == "sum":
            if not field:
                return {"success": False, "message": "Field for sum calculation not specified"}
            
            # Handle array fields
            if field == "scores":
                pipeline.extend([
                    {"$unwind": f"${field}"},
                    {"$group": {
                        "_id": None,
                        "total": {"$sum": f"${field}"}
                    }}
                ])
            else:
                pipeline.append({
                    "$group": {
                        "_id": None,
                        "total": {"$sum": f"${field}"}
                    }
                })

        return {
            "success": True,
            "query": pipeline,
            "type": "aggregate",
            "message": f"{operation_type} query generated successfully"
        }

    except Exception as e:
        print(f"Error in handle_nosql_aggregate_query: {str(e)}")
        return {"success": False, "message": f"Error generating aggregate query: {str(e)}"}

def handle_distinct_query(tokens, schema, collection_name):
    """Handle MongoDB distinct value queries"""
    try:
        field = None
        query = {}
        i = 1  # Skip first token

        # Find the field to get distinct values from
        while i < len(tokens):
            if tokens[i] == "where":
                # Handle conditions
                i += 1
                continue
            elif tokens[i] in schema:
                field = tokens[i]
                break
            elif "." in tokens[i]:  # Handle nested fields like "address.city"
                parts = tokens[i].split(".")
                if parts[0] in schema:
                    field = tokens[i]
                    break
            i += 1

        if not field:
            return {"success": False, "message": "Field for distinct values not specified"}

        return {
            "success": True,
            "query": {
                "field": field,
                "query": query
            },
            "type": "distinct",
            "message": "Distinct query generated successfully"
        }

    except Exception as e:
        print(f"Error in handle_distinct_query: {str(e)}")
        return {"success": False, "message": f"Error generating distinct query: {str(e)}"}

#####################################################
def handle_find_query(tokens, schema, operators, collection_name):
    """Handle MongoDB find queries with sorting and conditions"""
    query = {}
    sort = None
    fields = []
    
    i = 1  # Skip first token
    while i < len(tokens):
        if tokens[i] == "where":
            # Handle conditions
            i += 1
            condition_result = find_mongo_operator_and_value(tokens[i:], operators)
            if condition_result["success"]:
                query[tokens[i]] = condition_result["query"]
                i += condition_result["tokens_used"]
        elif tokens[i] == "order" and i + 1 < len(tokens) and tokens[i + 1] == "by":
            # Handle sorting
            i += 2
            sort = {tokens[i]: -1 if i + 1 < len(tokens) and tokens[i + 1] == "desc" else 1}
            break
        else:
            i += 1

    result = {
        "find": query
    }
    if sort:
        result["sort"] = sort

    return {
        "success": True,
        "query": result,
        "type": "find",
        "message": "Find query generated successfully"
    }

def find_mongo_operator_and_value(tokens, operators):
    """Parse MongoDB operators and values from tokens"""
    try:
        for op_phrase, mongo_op in operators.items():
            op_tokens = op_phrase.split()
            if tokens[:len(op_tokens)] == op_tokens:
                value_token = tokens[len(op_tokens):]
                if value_token:
                    try:
                        value = float(value_token[0]) if "." in value_token[0] else int(value_token[0])
                    except ValueError:
                        value = value_token[0]
                    return {
                        "success": True,
                        "query": {mongo_op: value},
                        "tokens_used": len(op_tokens) + 1
                    }
        return {"success": False, "message": "No valid operator or value found"}
    except Exception as e:
        return {"success": False, "message": str(e)}







#############################################################################
def handle_mongodb_find(tokens, schema):
    """Handle MongoDB find queries"""
    try:
        query = {}
        projection = {}
        sort = {}
        
        i = 1  # Skip first token
        while i < len(tokens):
            # Handle specific field selection
            if tokens[i] in schema:
                projection[tokens[i]] = 1
                i += 1
                continue
                
            # Handle WHERE conditions
            if tokens[i] == 'where':
                conditions = parse_mongodb_conditions(tokens[i+1:], schema)
                if conditions["success"]:
                    query = conditions["query"]
                    i += conditions["tokens_used"] + 1
                    continue
                    
            # Handle sorting
            if tokens[i] in ['order', 'sort'] and i + 2 < len(tokens):
                if tokens[i+1] == 'by' and tokens[i+2] in schema:
                    direction = 1  # Default ascending
                    if i + 3 < len(tokens) and tokens[i+3] == 'desc':
                        direction = -1
                    sort[tokens[i+2]] = direction
                    i += 4
                    continue
                    
            i += 1
            
        result = {
            "operation": "find",
            "query": query,
            "options": {}
        }
        
        if projection:
            result["options"]["projection"] = projection
        if sort:
            result["options"]["sort"] = sort
            
        return {
            "success": True,
            "query": result,
            "type": "find"
        }
        
    except Exception as e:
        return {"success": False, "message": f"Error in find: {str(e)}"}

def handle_mongodb_update(tokens, schema):
    """Handle MongoDB update queries"""
    try:
        update_ops = {"$set": {}}
        filter_query = {}
        
        i = 1  # Skip update
        while i < len(tokens):
            if tokens[i] in schema:
                field = tokens[i]
                if i + 2 < len(tokens) and tokens[i+1] in ['to', '=']:
                    value = tokens[i+2]
                    try:
                        value = float(value) if '.' in value else int(value)
                    except ValueError:
                        value = value.strip("'\"")
                    update_ops["$set"][field] = value
                    i += 3
                    continue
                    
            if tokens[i] == 'where':
                conditions = parse_mongodb_conditions(tokens[i+1:], schema)
                if conditions["success"]:
                    filter_query = conditions["query"]
                    break
                    
            i += 1
            
        return {
            "success": True,
            "query": {
                "operation": "update_many",
                "filter": filter_query,
                "update": update_ops
            },
            "type": "update"
        }
        
    except Exception as e:
        return {"success": False, "message": f"Error in update: {str(e)}"}

def handle_mongodb_delete(tokens, schema):
    """Handle MongoDB delete queries"""
    try:
        filter_query = {}
        
        i = 1  # Skip delete
        while i < len(tokens):
            if tokens[i] == 'where':
                conditions = parse_mongodb_conditions(tokens[i+1:], schema)
                if conditions["success"]:
                    filter_query = conditions["query"]
                    break
            i += 1
            
        return {
            "success": True,
            "query": {
                "operation": "delete_many",
                "filter": filter_query
            },
            "type": "delete"
        }
        
    except Exception as e:
        return {"success": False, "message": f"Error in delete: {str(e)}"}

def handle_mongodb_aggregate(tokens, schema):
    """Handle MongoDB aggregate queries"""
    try:
        pipeline = []
        i = 1  # Skip first token
        
        # Handle different aggregate functions
        agg_funcs = {
            'average': '$avg',
            'sum': '$sum',
            'minimum': '$min',
            'maximum': '$max',
            'count': '$sum'
        }
        
        while i < len(tokens):
            if tokens[i] in agg_funcs:
                func = agg_funcs[tokens[i]]
                if i + 2 < len(tokens) and tokens[i+1] == 'of' and tokens[i+2] in schema:
                    field = tokens[i+2]
                    
                    # Build group stage
                    group_stage = {
                        "$group": {
                            "_id": None,
                            "result": {func: f"${field}"}
                        }
                    }
                    
                    # Look for GROUP BY
                    j = i + 3
                    while j < len(tokens):
                        if tokens[j] == 'group' and j + 2 < len(tokens) and tokens[j+1] == 'by' and tokens[j+2] in schema:
                            group_stage["$group"]["_id"] = f"${tokens[j+2]}"
                            break
                        j += 1
                        
                    pipeline.append(group_stage)
                    
                    # Look for HAVING conditions
                    while j < len(tokens):
                        if tokens[j] == 'having':
                            having_conditions = parse_mongodb_having(tokens[j+1:], schema)
                            if having_conditions["success"]:
                                pipeline.append({"$match": having_conditions["query"]})
                            break
                        j += 1
                        
                    break
            i += 1
            
        return {
            "success": True,
            "query": {
                "operation": "aggregate",
                "pipeline": pipeline
            },
            "type": "aggregate"
        }
        
    except Exception as e:
        return {"success": False, "message": f"Error in aggregate: {str(e)}"}

def parse_mongodb_having(tokens, schema):
    """Parse HAVING conditions for MongoDB aggregation"""
    try:
        query = {}
        agg_funcs = {
            'count': '$sum',
            'sum': '$sum',
            'avg': '$avg',
            'average': '$avg',
            'min': '$min',
            'max': '$max',
            'minimum': '$min',
            'maximum': '$max'
        }
        
        i = 0
        while i < len(tokens):
            if tokens[i] in agg_funcs:
                func = agg_funcs[tokens[i]]
                if i + 2 < len(tokens):
                    # Handle operators
                    op = tokens[i+1]
                    value = tokens[i+2]
                    
                    # Convert operator to MongoDB syntax
                    mongo_op = {
                        '>': '$gt',
                        '<': '$lt',
                        '>=': '$gte',
                        '<=': '$lte',
                        '=': '$eq',
                        '!=': '$ne'
                    }.get(op, op)
                    
                    # Convert value to appropriate type
                    try:
                        value = float(value) if '.' in value else int(value)
                    except ValueError:
                        value = value.strip("'\"")
                        
                    query['result'] = {mongo_op: value}
                    i += 3
                    
                    # Handle AND/OR conditions
                    if i < len(tokens) and tokens[i] in ['and', 'or']:
                        if tokens[i] == 'or' and len(query) > 0:
                            query = {'$or': [query]}
                        i += 1
                        continue
                        
            i += 1
            
        return {
            "success": True,
            "query": query,
            "tokens_used": i
        }
        
    except Exception as e:
        return {"success": False, "message": f"Error in HAVING: {str(e)}"}

def handle_mongodb_aggregate(tokens, schema):
    """Enhanced MongoDB aggregate handler with HAVING support"""
    try:
        pipeline = []
        i = 1  # Skip first token
        
        # Aggregate function mapping
        agg_funcs = {
            'average': '$avg',
            'avg': '$avg',
            'sum': '$sum',
            'minimum': '$min',
            'maximum': '$max',
            'min': '$min',
            'max': '$max',
            'count': '$sum'
        }
        
        # Match stage (WHERE)
        while i < len(tokens):
            if tokens[i] == 'where':
                conditions = parse_mongodb_conditions(tokens[i+1:], schema)
                if conditions["success"]:
                    pipeline.append({"$match": conditions["query"]})
                    i += conditions["tokens_used"] + 1
                    break
            i += 1
            
        # Group stage
        group_stage = {"$group": {"_id": None}}
        current_field = None
        
        while i < len(tokens):
            if tokens[i] in agg_funcs:
                func = agg_funcs[tokens[i]]
                if i + 2 < len(tokens) and tokens[i+1] == 'of' and tokens[i+2] in schema:
                    field = tokens[i+2]
                    current_field = field
                    
                    # Handle count differently
                    if tokens[i] == 'count':
                        group_stage["$group"][f"{field}_count"] = {func: 1}
                    else:
                        group_stage["$group"][f"{field}_{tokens[i]}"] = {func: f"${field}"}
                        
                    i += 3
                    continue
                    
            # Handle GROUP BY
            if tokens[i] == 'group' and i + 2 < len(tokens) and tokens[i+1] == 'by':
                if tokens[i+2] in schema:
                    group_field = tokens[i+2]
                    group_stage["$group"]["_id"] = f"${group_field}"
                    i += 3
                    continue
                    
            # Handle HAVING
            if tokens[i] == 'having':
                having_result = parse_mongodb_having(tokens[i+1:], schema)
                if having_result["success"]:
                    pipeline.append(group_stage)
                    pipeline.append({"$match": having_result["query"]})
                    i += having_result["tokens_used"] + 1
                    continue
                    
            i += 1
            
        # Add group stage if not added by HAVING
        if pipeline and "$group" not in str(pipeline[-1]):
            pipeline.append(group_stage)
            
        # Add project stage to rename fields
        project_stage = {
            "$project": {
                "_id": 0
            }
        }
        
        for key in group_stage["$group"]:
            if key != "_id":
                project_stage["$project"][key.replace("_", " ")] = f"${key}"
                
        if len(project_stage["$project"]) > 1:
            pipeline.append(project_stage)
            
        return {
            "success": True,
            "query": {
                "operation": "aggregate",
                "pipeline": pipeline
            },
            "type": "aggregate"
        }
        
    except Exception as e:
        return {"success": False, "message": f"Error in aggregate: {str(e)}"}
    
def parse_mongodb_conditions(tokens, schema):
    """Parse conditions for MongoDB queries"""
    try:
        query = {}
        i = 0
        
        while i < len(tokens):
            if tokens[i] in schema:
                field = tokens[i]
                if i + 2 < len(tokens):
                    op = tokens[i+1]
                    value = tokens[i+2]
                    
                    # Convert operator
                    mongo_op = {
                        '>': '$gt',
                        '<': '$lt',
                        '>=': '$gte',
                        '<=': '$lte',
                        '=': '$eq',
                        '!=': '$ne'
                    }.get(op, op)
                    
                    # Convert value
                    try:
                        value = float(value) if '.' in value else int(value)
                    except ValueError:
                        value = value.strip("'\"")
                        
                    query[field] = {mongo_op: value}
                    i += 3
                    
                    # Handle AND/OR
                    if i < len(tokens) and tokens[i] in ['and', 'or']:
                        if tokens[i] == 'or' and len(query) > 0:
                            query = {'$or': [query]}
                        i += 1
                        continue
                        
            i += 1
            
        return {
            "success": True,
            "query": query,
            "tokens_used": i
        }
        
    except Exception as e:
        return {"success": False, "message": f"Error in conditions: {str(e)}"}
    

#####################################################################################    
def handle_update_query(tokens, schema, operators,table_name):
    """Handle UPDATE queries with improved operator handling"""
    try:
        if len(tokens) < 4:
            return {"success": False, "message": "Invalid UPDATE query format - insufficient tokens"}
            
        update_col = None
        update_value = None
        where_clause = ""
        
        i = 1  # Skip 'update'
        while i < len(tokens):
            # Handle SET clause
            if tokens[i] in schema:
                update_col = tokens[i]
                # Find value to update to using custom operators for UPDATE
                update_operators = {'to': '=', '=': '=', 'as': '=', 'with': '='}
                value_result = find_operator_and_value(tokens[i+1:], update_operators)
                
                if value_result["success"]:
                    update_value = value_result["value"]
                    # Convert value to proper format
                    if isinstance(update_value, (int, float)):
                        update_value = str(update_value)
                    else:
                        update_value = f"'{update_value}'"
                    i += value_result["tokens_used"] + 1
                    continue
                else:
                    return {"success": False, "message": "Invalid SET value format"}
                    
            # Handle WHERE clause
            if tokens[i] == 'where':
                if i + 1 < len(tokens) and tokens[i + 1] in schema:
                    condition_col = tokens[i + 1]
                    # Find operator and value for WHERE condition
                    condition_result = find_operator_and_value(tokens[i+2:], operators)
                    
                    if condition_result["success"]:
                        condition_value = condition_result["value"]
                        # Format condition value
                        if isinstance(condition_value, (int, float)):
                            condition_value = str(condition_value)
                        else:
                            condition_value = f"'{condition_value}'"
                            
                        where_clause = f"{condition_col} {condition_result['operator']} {condition_value}"
                        i += condition_result["tokens_used"] + 2
                    else:
                        return {"success": False, "message": "Invalid WHERE condition format"}
                else:
                    return {"success": False, "message": f"Invalid column in WHERE clause: {tokens[i+1] if i+1 < len(tokens) else 'missing'}"}
                
            i += 1
            
        # Validate required components
        if not update_col:
            return {"success": False, "message": "Missing column to update"}
        if update_value is None:
            return {"success": False, "message": "Missing value to update to"}
            
        # Build query
        query = f"UPDATE {table_name} SET {update_col} = {update_value};"
        if where_clause:
            query += f" WHERE {where_clause}"
            
        return {
            "success": True,
            "query": f"{query};",
            "type": "update",
            "message": "Update query generated successfully"
        }
        
    except Exception as e:
        print(f"Error in handle_update_query: {str(e)}")
        return {"success": False, "message": f"Error handling UPDATE: {str(e)}"}

#####################################################################################


######################################################################################
def parse_where_clause(tokens, schema):
    """Parse WHERE clause conditions"""
    try:
        conditions = []
        distinct_cols = []
        i = 0
        
        while i < len(tokens):
            if tokens[i] == 'distinct':
                i += 1
                if i < len(tokens) and tokens[i] in schema:
                    distinct_cols.append(tokens[i])
                    i += 1
                continue
            
            if tokens[i] in schema:
                col = tokens[i]
                if i + 2 < len(tokens):
                    op = get_operator(tokens[i+1])
                    value = tokens[i+2]
                    conditions.append(f"{col} {op} {value}")
                    i += 3
                    
                    if i < len(tokens) and tokens[i] in ['and', 'or']:
                        conditions.append(tokens[i].upper())
                        i += 1
                        continue
            i += 1
            
        if not conditions and not distinct_cols:
            return {"success": False, "message": "Invalid WHERE clause"}
            
        return {
            "success": True,
            "distinct_cols": distinct_cols,
            "clause": " ".join(conditions),
            "tokens_used": i
        }
        
    except Exception as e:
        return {"success": False, "message": f"Error in WHERE: {str(e)}"}

def get_operator(token):
    """Convert natural language operators to SQL operators"""
    operators = {
        'equals': '=',
        'equal': '=',
        'is': '=',
        'greater': '>',
        'less': '<',
        'than': '',
        '>': '>',
        '<': '<',
        '=': '='
    }
    return operators.get(token, token)

# Add helper functions for GROUP BY, ORDER BY, and HAVING clauses
def parse_group_by(tokens, schema):
    """Parse GROUP BY clause with improved validation"""
    try:
        cols = []
        i = 0
        while i < len(tokens):
            if tokens[i] in schema:
                cols.append(tokens[i])
                i += 1
                # Handle multiple columns separated by 'and'
                if i < len(tokens) and tokens[i] == 'and':
                    i += 1
                    continue
            else:
                break
        
        if not cols:
            return {
                "success": False,
                "message": "No valid columns found for GROUP BY"
            }
            
        return {
            "success": True,
            "clause": ", ".join(cols),
            "tokens_used": i
        }
        
    except Exception as e:
        return {"success": False, "message": f"Error in GROUP BY: {str(e)}"}

def parse_order_by(tokens, schema):
    """Parse ORDER BY clause with improved direction handling"""
    try:
        result = []
        i = 0
        while i < len(tokens):
            if tokens[i] in schema:
                column = tokens[i]
                direction = "ASC"  # default direction
                
                # Check for direction specifier
                if i + 1 < len(tokens) and tokens[i + 1] in ['asc', 'desc', 'ascending', 'descending']:
                    direction = "DESC" if tokens[i + 1] in ['desc', 'descending'] else "ASC"
                    i += 2
                else:
                    i += 1
                    
                result.append(f"{column} {direction}")
                
                # Handle multiple columns
                if i < len(tokens) and tokens[i] == 'and':
                    i += 1
                    continue
            else:
                break
                
        if not result:
            return {
                "success": False,
                "message": "No valid columns found for ORDER BY"
            }
            
        return {
            "success": True,
            "clause": ", ".join(result),
            "tokens_used": i
        }
        
    except Exception as e:
        return {"success": False, "message": f"Error in ORDER BY: {str(e)}"}

def parse_having_with_operators(tokens, schema, operators):
    """Parse HAVING clause with improved operator handling"""
    try:
        conditions = []
        i = 0
        agg_funcs = {'count', 'sum', 'avg', 'min', 'max', 'average', 'minimum', 'maximum'}
        
        while i < len(tokens):
            if tokens[i] in agg_funcs:
                func = tokens[i].upper()
                if func == 'AVERAGE':
                    func = 'AVG'
                elif func in ['MINIMUM', 'MAXIMUM']:
                    func = 'MIN' if func == 'MINIMUM' else 'MAX'
                
                if i + 2 < len(tokens) and tokens[i+1] == 'of' and tokens[i+2] in schema:
                    column = tokens[i+2]
                    agg_expr = f"{func}({column})"
                    
                    # Find operator and value
                    op_result = find_operator_and_value(tokens[i+3:], operators)
                    if op_result["success"]:
                        conditions.append(f"{agg_expr} {op_result['operator']} {op_result['value']}")
                        i += op_result["tokens_used"] + 3
                        
                        # Handle AND/OR
                        if i < len(tokens) and tokens[i] in ['and', 'or']:
                            conditions.append(tokens[i].upper())
                            i += 1
                            continue
                    else:
                        return op_result
                        
            i += 1
            
        if not conditions:
            return {
                "success": False,
                "message": "No valid conditions found for HAVING"
            }
            
        return {
            "success": True,
            "clause": " ".join(conditions),
            "tokens_used": i
        }
        
    except Exception as e:
        return {"success": False, "message": f"Error in HAVING: {str(e)}"}



def parse_having_clause(tokens, schema):
    """Parse HAVING clause with aggregate conditions"""
    try:
        agg_funcs = {'count', 'sum', 'avg', 'min', 'max', 'average', 'minimum', 'maximum'}
        conditions = []
        i = 0
        
        while i < len(tokens):
            # Check for aggregate function
            if tokens[i] in agg_funcs:
                agg_func = tokens[i].upper()
                if agg_func == 'AVERAGE':
                    agg_func = 'AVG'
                elif agg_func in ['MINIMUM', 'MAXIMUM']:
                    agg_func = 'MIN' if agg_func == 'MINIMUM' else 'MAX'
                    
                if i + 1 < len(tokens):
                    # Handle "count records" or "count *" cases
                    if tokens[i] == 'count' and tokens[i+1] in ['records', '*']:
                        conditions.append(f"COUNT(*)")
                        i += 2
                    # Handle other aggregate functions
                    elif i + 2 < len(tokens) and tokens[i+1] == 'of' and tokens[i+2] in schema:
                        conditions.append(f"{agg_func}({tokens[i+2]})")
                        i += 3
                        
                    # Look for operator and value
                    if i + 2 < len(tokens):
                        op = get_operator(tokens[i])
                        value = tokens[i+1]
                        conditions.append(f"{op} {value}")
                        i += 2
                        
                        # Handle AND/OR connectors
                        if i < len(tokens) and tokens[i] in ['and', 'or']:
                            conditions.append(tokens[i].upper())
                            i += 1
            i += 1
            
        if not conditions:
            return {"success": False, "message": "Invalid HAVING clause"}
            
        return {
            "success": True,
            "clause": " ".join(conditions),
            "tokens_used": i
        }
        
    except Exception as e:
        return {"success": False, "message": f"Error in HAVING: {str(e)}"}

def handle_delete_query(tokens, schema, operators):
    """Handle DELETE queries with improved operator handling"""
    try:
        where_clause = ""
        i = 1  # Skip 'delete'
        
        # Look for FROM keyword (optional)
        while i < len(tokens):
            if tokens[i] == 'from':
                i += 1
                break
            if tokens[i] == 'where':  # We hit WHERE before FROM, that's fine
                break
            i += 1
            
        # Look for WHERE clause
        while i < len(tokens):
            if tokens[i] == 'where':
                if i + 1 >= len(tokens):
                    return {"success": False, "message": "WHERE clause is incomplete"}
                    
                if tokens[i + 1] not in schema:
                    return {
                        "success": False,
                        "message": f"Invalid column in WHERE clause: {tokens[i+1]}"
                    }
                    
                column = tokens[i + 1]
                op_result = find_operator_and_value(tokens[i+2:], operators)
                
                if op_result["success"]:
                    # Format the value appropriately
                    value = op_result["value"]
                    if isinstance(value, (int, float)):
                        formatted_value = str(value)
                    else:
                        formatted_value = f"'{value}'"
                        
                    where_clause = f"{column} {op_result['operator']} {formatted_value}"
                    i += op_result["tokens_used"] + 2
                else:
                    return {
                        "success": False,
                        "message": f"Invalid operator or value in WHERE clause: {' '.join(tokens[i+2:])}"
                    }
                
                # Check for additional conditions (AND/OR)
                while i < len(tokens):
                    if tokens[i] in ['and', 'or']:
                        if i + 1 >= len(tokens) or tokens[i + 1] not in schema:
                            return {"success": False, "message": f"Invalid {tokens[i].upper()} condition"}
                            
                        column = tokens[i + 1]
                        op_result = find_operator_and_value(tokens[i+2:], operators)
                        
                        if op_result["success"]:
                            value = op_result["value"]
                            if isinstance(value, (int, float)):
                                formatted_value = str(value)
                            else:
                                formatted_value = f"'{value}'"
                                
                            where_clause += f" {tokens[i].upper()} {column} {op_result['operator']} {formatted_value}"
                            i += op_result["tokens_used"] + 2
                        else:
                            return {
                                "success": False,
                                "message": f"Invalid operator or value in {tokens[i].upper()} condition"
                            }
                    else:
                        break
                        
                break
            i += 1
            
        # Validate WHERE clause existence for safety
        if not where_clause:
            return {
                "success": False,
                "message": "DELETE query requires a WHERE clause for safety"
            }
            
        # Build query
        query = "DELETE FROM student_perf"
        query += f" WHERE {where_clause}"
            
        return {
            "success": True,
            "query": f"{query};",
            "type": "delete",
            "message": "Delete query generated successfully"
        }
        
    except Exception as e:
        print(f"Error in handle_delete_query: {str(e)}")
        return {"success": False, "message": f"Error handling DELETE: {str(e)}"}

def handle_insert_query(tokens, schema):
    """Handle INSERT queries"""
    try:
        columns = []
        values = []
        i = 1  # Skip 'insert'
        
        # Handle different insert formats
        if tokens[i] == 'into':
            i += 1
            
        # Parse column-value pairs
        while i < len(tokens):
            if tokens[i] in schema:
                col = tokens[i]
                if i + 2 < len(tokens) and tokens[i+1] in ['=', 'is', 'as']:
                    val = tokens[i+2]
                    columns.append(col)
                    # Handle string values
                    if not val.replace('.', '').isdigit():
                        val = f"'{val}'"
                    values.append(val)
                    i += 3
                    
                    # Skip 'and' connector
                    if i < len(tokens) and tokens[i] == 'and':
                        i += 1
                        continue
            i += 1
            
        if not columns or not values:
            return {"success": False, "message": "No valid column-value pairs found"}
            
        # Build query
        query = f"INSERT INTO student_perf ({', '.join(columns)}) VALUES ({', '.join(values)})"
        
        return {
            "success": True,
            "query": f"{query};",
            "type": "insert"
        }
        
    except Exception as e:
        return {"success": False, "message": f"Error in INSERT: {str(e)}"}

# Helper function for multiple value INSERT
def parse_multiple_values(tokens, schema):
    """Parse multiple sets of values for INSERT"""
    value_sets = []
    current_set = []
    i = 0
    
    while i < len(tokens):
        if tokens[i] == 'and' and current_set:
            value_sets.append(current_set)
            current_set = []
        elif tokens[i].replace('.', '').isdigit():
            current_set.append(tokens[i])
        elif tokens[i] not in ['values', 'with']:
            current_set.append(f"'{tokens[i]}'")
        i += 1
        
    if current_set:
        value_sets.append(current_set)
        
    return value_sets

##########################################################



def convert_to_mongodb_query(sql_query, operation_type):
    """Convert SQL query to MongoDB query format"""
    if operation_type == "select":
        # Handle SELECT queries
        if "WHERE" in sql_query:
            # Extract conditions and convert operators
            conditions = sql_query.split("WHERE")[1].split("GROUP BY")[0].strip()
            return {"$match": parse_sql_conditions(conditions)}
        else:
            return {}  # Empty query returns all documents

    elif operation_type == "update":
        # Handle UPDATE queries
        set_clause = sql_query.split("SET")[1].split("WHERE")[0].strip()
        update_dict = {"$set": parse_sql_set_clause(set_clause)}
        if "WHERE" in sql_query:
            conditions = sql_query.split("WHERE")[1].strip()
            return {"filter": parse_sql_conditions(conditions), "update": update_dict}
        return {"filter": {}, "update": update_dict}

    elif operation_type == "delete":
        # Handle DELETE queries
        if "WHERE" in sql_query:
            conditions = sql_query.split("WHERE")[1].strip()
            return parse_sql_conditions(conditions)
        return {}

    elif operation_type == "aggregate":
        # Handle aggregate queries
        pipeline = []
        if "WHERE" in sql_query:
            conditions = sql_query.split("WHERE")[1].split("GROUP BY")[0].strip()
            pipeline.append({"$match": parse_sql_conditions(conditions)})
        if "GROUP BY" in sql_query:
            group_by = sql_query.split("GROUP BY")[1].split("HAVING")[0].strip()
            pipeline.append({"$group": parse_sql_group_by(group_by)})
        return pipeline

    return {}  # Default empty query

def parse_sql_conditions(conditions):
    """Convert SQL WHERE conditions to MongoDB format"""
    mongo_conditions = {}
    # Basic conversion of operators
    operators = {
        "=": "$eq",
        ">": "$gt",
        "<": "$lt",
        ">=": "$gte",
        "<=": "$lte",
        "!=": "$ne"
    }
    # Split conditions and convert
    for condition in conditions.split("AND"):
        parts = condition.strip().split()
        if len(parts) >= 3:
            field = parts[0]
            op = operators.get(parts[1], parts[1])
            value = parts[2].strip(";")
            try:
                value = float(value) if '.' in value else int(value)
            except ValueError:
                value = value.strip("'\"")
            mongo_conditions[field] = {op: value}
    return mongo_conditions

def parse_sql_set_clause(set_clause):
    """Convert SQL SET clause to MongoDB format"""
    updates = {}
    parts = set_clause.split(",")
    for part in parts:
        field, value = part.split("=")
        field = field.strip()
        value = value.strip().strip(";")
        try:
            value = float(value) if '.' in value else int(value)
        except ValueError:
            value = value.strip("'\"")
        updates[field] = value
    return updates

def parse_sql_group_by(group_by):
    """Convert SQL GROUP BY clause to MongoDB format"""
    fields = group_by.split(",")
    group_dict = {"_id": {}}
    for field in fields:
        field = field.strip()
        group_dict["_id"][field] = f"${field}"
    return group_dict


###################################################################################
#1. Break NLQ into tokens and normalize with the help of keyword_mapping file
def preprocess_query(nl_query):
    import re

    # Step 1: Normalize synonyms using keyword_mapping
    normalized_query = nl_query.lower().strip()  # Convert to lowercase and strip extra spaces
    
    # Use keyword_mapping to replace synonyms with standard terms
    for key, synonyms in keyword_mapping.items():
        for synonym in sorted(synonyms, key=len, reverse=True):  # Sort by length to replace longer synonyms first
            pattern = r'\b' + re.escape(synonym.lower()) + r'\b'  # Match whole words only
            normalized_query = re.sub(pattern, key, normalized_query)
    
    # Replace specific phrases
    normalized_query = normalized_query.replace("rows where", "where")
    
    # Step 2: Remove redundant spaces or special characters
    normalized_query = re.sub(r'\s+', ' ', normalized_query)  # Replace multiple spaces with a single space
    normalized_query = re.sub(r'[^\w\s]', '', normalized_query)  # Remove non-alphanumeric characters
    
    return normalized_query
########################################################################################

#################################################################################
#2. detect whether NLQ corresponds to an SQL SELECT, INSERT.. operation default to SELECT

def identify_operation_type(query):
    """Identify the type of operation from the normalized query"""
    operation_keywords = QueryConstructs.get_operation_keywords()
    query_words = set(query.split())
    for op_type, keywords in operation_keywords.items():
        if any(keyword in query_words for keyword in keywords):
            return op_type
    # Default to SELECT if no specific operation is identified
    return "SELECT"
#####################################################################################

#############################################
#3. Validate that fields mentioend in NLQ exists in the schema

def validate_variables(variables, schema):
    """Validate that variables reference valid columns"""
    for var_name, value in variables.items():
        if var_name.endswith('_col') or var_name in ['column', 'column1', 'column2']:
            if value not in schema:
                return False
    return True

#############################################




import re

def escape_special_characters(template):
    """Escape special characters in the template"""
    return re.escape(template)






def process_mongodb_template(template, table_name, variables):
    """Process MongoDB template with variables"""
    if isinstance(template, list):
        # Handle pipeline
        pipeline = []
        for stage in template:
            stage_str = json.dumps(stage)
            for var_name, value in variables.items():
                stage_str = stage_str.replace(f"${{{var_name}}}", value)
                stage_str = stage_str.replace(f"{{{var_name}}}", value)
            pipeline.append(json.loads(stage_str))
        return pipeline
    else:
        # Handle single query
        query_str = json.dumps(template)
        for var_name, value in variables.items():
            query_str = query_str.replace(f"${{{var_name}}}", value)
            query_str = query_str.replace(f"{{{var_name}}}", value)
        return json.loads(query_str)



##################################################################################
#helper function for process_nl_query-savepoint3    
def fetch_schema(db_type, table_name):
    """
    Fetches the schema for a given table/collection based on the database type.
    """
    if db_type == "mysql":
        # Fetch schema from MySQL
        connection = mysql_create_connection()
        cursor = connection.cursor()
        try:
            cursor.execute(f"DESCRIBE `{table_name}`;")
            schema = [row[0] for row in cursor.fetchall()]  # Extract column names
            return schema
        except Exception as e:
            raise Exception(f"Error fetching MySQL schema: {str(e)}")
        finally:
            mysql_close_connection(connection)

    elif db_type == "mongodb":
        # Fetch schema from MongoDB
        client, db = mongo_create_connection()
        try:
            collection = db[table_name]
            sample_document = collection.find_one()
            if sample_document:
                schema = list(sample_document.keys())
                schema.remove('_id')  # Exclude '_id' field
                return schema
            else:
                raise Exception(f"Collection '{table_name}' is empty.")
        except Exception as e:
            raise Exception(f"Error fetching MongoDB schema: {str(e)}")
        finally:
            mongo_close_connection(client)

    else:
        raise ValueError(f"Unsupported database type: {db_type}")

#######################################################################################

@app.route('/execute-query', methods=['POST'])
def execute_query():
    try:
        db_type = request.json.get("db_type")
        query = request.json.get("query")
        table_name = request.json.get("table_name")
        operation = request.json.get("operation", "find")  # Default to find operation
        print("db_type:", db_type)
        print("operation:", operation)
        print("query:", query)
        print("table_name:", table_name)

        if not db_type:
            return jsonify({"success": False, "message": "Missing 'db_type' parameter."}), 400
        if not table_name:
            return jsonify({"success": False, "message": "Missing 'table_name' parameter."}), 400
        if not query:
            return jsonify({"success": False, "message": "Missing 'query' parameter."}), 400
        
        # Execute MongoDB query
        if db_type == "mongodb":
            client, db = mongo_create_connection()
            collection = db[table_name]
            
            if operation.lower() == "find":
                # Ensure query is a dictionary
                if not isinstance(query, dict):
                    return jsonify({"success": False, "message": "Filter for `find` must be a dictionary.Did you mean to use 'aggregate' operation?"}), 400
                results = list(collection.find(query))
            elif operation.lower() == "aggregate":
                # Ensure query is a list
                if not isinstance(query, list):
                    return jsonify({"success": False, "message": "Pipeline for `aggregate` must be a list."}), 400
                results = list(collection.aggregate(query))
            else:
                return jsonify({"success": False, "message": f"Unsupported operation: {operation}"}), 400

            # Convert ObjectId to string for all results
            for doc in results:
                if "_id" in doc:
                    doc["_id"] = str(doc["_id"])

            mongo_close_connection(client)
            return jsonify({
                "success": True,
                "message": "Query executed successfully.",
                "results": results
            })

        # Execute MySQL query
        elif db_type == "mysql":
            connection = mysql_create_connection()
            cursor = connection.cursor()
            cursor.execute(query)
            
            if query.strip().lower().startswith('select'):
                rows = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]
                mysql_close_connection(connection)
                return jsonify({
                    "success": True,
                    "message": "Query executed successfully.",
                    "columns": columns,
                    "rows": rows
                })
            else:
                connection.commit()
                mysql_close_connection(connection)
                return jsonify({
                    "success": True,
                    "message": "Query executed successfully.",
                    "rowcount": cursor.rowcount
                })

        else:
            return jsonify({"success": False, "message": "Unsupported database type."}), 400

    except Exception as e:
        print(f"Error executing query: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({
            "success": False,
            "message": f"Error executing query: {str(e)}"
        }), 500

if __name__ == "__main__":
    app.run(debug=True)
