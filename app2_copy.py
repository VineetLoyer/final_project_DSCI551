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
    try:
        # Fetch input parameters
        db_type = request.json.get("db_type")
        nl_query = request.json.get("nl_query")
        table_name = request.json.get("table_name")

        # Validate input
        if not all([nl_query, db_type, table_name]):
            return jsonify({"success": False, "message": "Missing required parameters"}), 400

        # Fetch schema
        try:
            schema = fetch_schema(db_type, table_name)
            if not schema:
                return jsonify({"success": False, "message": f"No schema found for table: {table_name}"}), 400
        except Exception as e:
            return jsonify({"success": False, "message": f"Error fetching schema: {str(e)}"}), 500

        # Initialize translator
        translator = QueryTranslator(keyword_mapping)

        # Generate query based on database type
        if db_type.lower() == "mongodb":
            result = translator.translate_to_mongodb(nl_query, schema)
        else:
            result = translator.translate_to_sql(nl_query, table_name, schema)

        if not result["success"]:
            return jsonify(result), 400

        return jsonify(result)

    except Exception as e:
        return jsonify({"success": False, "message": f"Error processing query: {str(e)}"}), 500


################################################################################################

#################################################################################    
class QueryTranslator:
    def __init__(self, keyword_mapping):
        self.keyword_mapping = keyword_mapping
        self.operators = {
            'greater than': '>',
            'is greater than': '>',
            'higher than': '>',
            'less than': '<',
            'lower than': '<',
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
        
        self.mongo_operators = {
            '>': '$gt',
            '<': '$lt',
            '=': '$eq',
            '!=': '$ne',
            '>=': '$gte',
            '<=': '$lte'
        }
        
        self.agg_functions = {
            'count': {'sql': 'COUNT', 'mongo': '$sum'},
            'average': {'sql': 'AVG', 'mongo': '$avg'},
            'sum': {'sql': 'SUM', 'mongo': '$sum'},
            'maximum': {'sql': 'MAX', 'mongo': '$max'},
            'minimum': {'sql': 'MIN', 'mongo': '$min'}
        }

    def translate_to_sql(self, nl_query: str, table_name: str, schema: list) -> dict:
        """Translate natural language query to SQL"""
        try:
            tokens = nl_query.lower().strip().split()
            query_type = self._identify_query_type(tokens)
            
            # Handle SELECT and its variations
            if query_type == "select":
                return self._build_sql_select(tokens, table_name, schema)
                
            # Handle aggregations
            elif query_type in self.agg_functions:
                return self._build_sql_aggregate(tokens, table_name, schema, query_type)
            
            # Handle GROUP BY
            elif any(word in self.keyword_mapping["group"] for word in tokens):
                return self._build_sql_group_by(tokens, table_name, schema)
            
            return {"success": False, "message": "Unsupported query type"}
            
        except Exception as e:
            return {"success": False, "message": f"Error translating to SQL: {str(e)}"}

    def translate_to_mongodb(self, nl_query: str, schema: list) -> dict:
        """Translate natural language query to MongoDB query"""
        try:
            tokens = nl_query.lower().strip().split()
            query_type = self._identify_query_type(tokens)
            
            # Handle find queries (equivalent to SELECT)
            if query_type == "select":
                return self._build_mongo_find(tokens, schema)
                
            # Handle aggregations
            elif query_type in self.agg_functions:
                return self._build_mongo_aggregate(tokens, schema, query_type)
            
            # Handle GROUP BY
            elif any(word in self.keyword_mapping["group"] for word in tokens):
                return self._build_mongo_group(tokens, schema)
            
            return {"success": False, "message": "Unsupported query type"}
            
        except Exception as e:
            return {"success": False, "message": f"Error translating to MongoDB: {str(e)}"}

    def _identify_query_type(self, tokens):
    # First check for explicit aggregate keywords
        aggregate_types = {
            "average": ["average", "avg", "mean"],
            "count": ["count", "total", "number of"],
            "sum": ["sum", "total of", "sum of"],
            "maximum": ["maximum", "max", "highest"],
            "minimum": ["minimum", "min", "lowest"]
        }
        
        query_text = " ".join(tokens)
        
        # Check for aggregate patterns first
        for agg_type, keywords in aggregate_types.items():
            if any(keyword in query_text for keyword in keywords):
                return agg_type
                
        # Check for other query types if no aggregation found
        for token in tokens:
            for query_type, keywords in self.keyword_mapping.items():
                if token in keywords:
                    if query_type in ["select", "count", "average", "sum", "maximum", "minimum"]:
                        return query_type
                        
        return "select"  # default


    def _normalize_schema(self, schema):
   
        return {col.lower(): col for col in schema}

    def _build_sql_select(self, tokens, table_name, schema):
        """Build SQL SELECT query with improved WHERE clause handling"""
        schema_map = self._normalize_schema(schema)
        query_text = " ".join(tokens).lower()
        
        # Initialize components
        select_cols = []
        where_conditions = []
        order_by = []
        
        # Handle column selection
        for col in schema:
            if col.lower() in query_text:
                select_cols.append(col)
        
        # If no specific columns mentioned, use all
        if not select_cols:
            select_cols = ["*"]
        
        # Handle WHERE conditions
        if "where" in query_text:
            where_part = query_text.split("where")[1].split("order by")[0] if "order by" in query_text else query_text.split("where")[1]
            
            # Check each column for conditions
            for col in schema:
                if col.lower() in where_part:
                    # Find the value and operator
                    col_index = where_part.index(col.lower())
                    remaining_text = where_part[col_index + len(col):].strip()
                    
                    # Handle different operators
                    for op_text, op_symbol in self.operators.items():
                        if remaining_text.startswith(op_text) or remaining_text.startswith(op_symbol):
                            op_len = len(op_text) if remaining_text.startswith(op_text) else len(op_symbol)
                            value_part = remaining_text[op_len:].strip().split()[0]
                            
                            # Handle numeric and string values
                            try:
                                numeric_value = float(value_part)
                                where_conditions.append(f"{col} {op_symbol} {numeric_value}")
                            except ValueError:
                                where_conditions.append(f"{col} {op_symbol} '{value_part}'")
                            break
        
        # Handle ORDER BY
        if "order by" in query_text:
            order_part = query_text.split("order by")[1].strip()
            for col in schema:
                if col.lower() in order_part:
                    direction = "DESC" if "desc" in order_part else "ASC"
                    order_by.append(f"{col} {direction}")
        
        # Build query
        query = f"SELECT {', '.join(select_cols)} FROM {table_name}"
        
        if where_conditions:
            query += f" WHERE {' AND '.join(where_conditions)}"
            
        if order_by:
            query += f" ORDER BY {', '.join(order_by)}"
            
        return {
            "success": True,
            "query": f"{query};",
            "type": "select"
        }

    def _build_mongo_find(self, tokens, schema):
        """Build MongoDB find query"""
        query = {}
        sort = {}
        projection = {}
        
        i = 0
        while i < len(tokens):
            # Handle field selection
            if tokens[i] in schema:
                projection[tokens[i]] = 1
                i += 1
                continue
                
            # Handle conditions
            if tokens[i] in self.keyword_mapping.get("where", []):
                i += 1
                while i < len(tokens) and tokens[i] in schema:
                    field = tokens[i]
                    for op_text, op_symbol in self.operators.items():
                        if i + 1 < len(tokens) and tokens[i + 1] == op_text:
                            value = tokens[i + 2]
                            try:
                                value = float(value)
                            except ValueError:
                                pass
                            query[field] = {self.mongo_operators[op_symbol]: value}
                            i += 3
                            break
                continue
                
            # Handle sorting
            if tokens[i] in self.keyword_mapping.get("order", []):
                i += 2  # Skip 'order' and 'by'
                while i < len(tokens) and tokens[i] in schema:
                    direction = -1 if i + 1 < len(tokens) and tokens[i + 1] in ["desc", "descending"] else 1
                    sort[tokens[i]] = direction
                    i += 2
                continue
                
            i += 1
            
        result = {"find": query}
        if projection:
            result["projection"] = projection
        if sort:
            result["sort"] = sort
            
        return {
            "success": True,
            "query": result,
            "type": "find"
        }

    def _build_sql_aggregate(self, tokens, table_name, schema, agg_type):
        """Build SQL aggregate query with enhanced column detection"""
        query_text = " ".join(tokens)
        
        # Find the column to aggregate
        agg_col = None
        if agg_type == "count":
            agg_col = "*"
        else:
            # Look for schema columns in the query
            for col in schema:
                if col in query_text:
                    agg_col = col
                    break
        
        if not agg_col and agg_type != "count":
            return {"success": False, "message": "No column specified for aggregation"}
        
        agg_func = {
            "average": "AVG",
            "count": "COUNT",
            "sum": "SUM",
            "maximum": "MAX",
            "minimum": "MIN"
        }[agg_type]
        
        query = f"SELECT {agg_func}({agg_col}) FROM {table_name}"
        
        # Add GROUP BY if present
        group_cols = []
        if "group by" in query_text:
            for col in schema:
                if f"group by {col}" in query_text:
                    group_cols.append(col)
            if group_cols:
                query += f" GROUP BY {', '.join(group_cols)}"
        
        # Add ORDER BY if present
        if "order by" in query_text:
            direction = "DESC" if "desc" in query_text else "ASC"
            order_by = f" ORDER BY {agg_func}({agg_col}) {direction}"
            query += order_by
        
        return {
            "success": True,
            "query": f"{query};",
            "type": agg_type
        }

    def _build_mongo_aggregate(self, tokens, schema, agg_type):
        """Build MongoDB aggregate query"""
        pipeline = []
        match_stage = {}
        group_stage = {"_id": None}
        
        # Find aggregation column
        agg_col = next((col for col in schema if col in tokens), None)
        if not agg_col and agg_type != "count":
            return {"success": False, "message": "No column specified for aggregation"}
            
        # Parse query
        i = 0
        group_by = None
        while i < len(tokens):
            # Handle WHERE conditions
            if tokens[i] in self.keyword_mapping.get("where", []):
                i += 1
                while i < len(tokens) and tokens[i] in schema:
                    field = tokens[i]
                    for op_text, op_symbol in self.operators.items():
                        if i + 1 < len(tokens) and tokens[i + 1] == op_text:
                            value = tokens[i + 2]
                            try:
                                value = float(value)
                            except ValueError:
                                pass
                            match_stage[field] = {self.mongo_operators[op_symbol]: value}
                            i += 3
                            break
                continue
                
            # Handle GROUP BY
            if tokens[i] in self.keyword_mapping.get("group", []):
                i += 2  # Skip 'group' and 'by'
                if i < len(tokens) and tokens[i] in schema:
                    group_by = tokens[i]
                    group_stage["_id"] = f"${group_by}"
                    i += 1
                continue
                
            i += 1
            
        # Build pipeline
        if match_stage:
            pipeline.append({"$match": match_stage})
            
        if agg_type == "count":
            group_stage["count"] = {"$sum": 1}
        else:
            group_stage[f"{agg_type}_result"] = {
                self.agg_functions[agg_type]['mongo']: f"${agg_col}"
            }
            
        pipeline.append({"$group": group_stage})
        
        return {
            "success": True,
            "query": pipeline,
            "type": "aggregate"
        }

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
    connection = None
    try:
        # Fetch input parameters
        db_type = request.json.get("db_type")
        query = request.json.get("query")
        table_name = request.json.get("table_name")
        operation = request.json.get("operation", "find")

        if not all([db_type, table_name, query]):
            return jsonify({"success": False, "message": "Missing required parameters."}), 400
        
        # Handle MongoDB queries
        if db_type == "mongodb":
            try:
                client, db = mongo_create_connection()
                collection = db[table_name]
                
                try:
                    if operation == "find":
                        if not isinstance(query, dict):
                            return jsonify({"success": False, "message": "Filter for `find` must be a dictionary."}), 400
                        results = list(collection.find(query))
                    elif operation == "aggregate":
                        if not isinstance(query, list):
                            return jsonify({"success": False, "message": "Pipeline for `aggregate` must be a list."}), 400
                        results = list(collection.aggregate(query))
                    else:
                        return jsonify({"success": False, "message": f"Unsupported operation: {operation}"}), 400

                    # Convert ObjectId to string
                    for doc in results:
                        if "_id" in doc:
                            doc["_id"] = str(doc["_id"])

                    return jsonify({
                        "success": True,
                        "message": "Query executed successfully.",
                        "results": results
                    })
                finally:
                    mongo_close_connection(client)

            except Exception as e:
                return jsonify({
                    "success": False,
                    "message": f"MongoDB error: {str(e)}"
                }), 500

        # Handle MySQL queries
        elif db_type == "mysql":
            try:
                connection = mysql_create_connection()
                cursor = connection.cursor()
                
                try:
                    cursor.execute(query)
                    
                    if query.strip().lower().startswith('select'):
                        columns = [desc[0] for desc in cursor.description]
                        rows = cursor.fetchall()
                        return jsonify({
                            "success": True,
                            "message": "Query executed successfully.",
                            "columns": columns,
                            "rows": rows
                        })
                    else:
                        connection.commit()
                        return jsonify({
                            "success": True,
                            "message": "Query executed successfully.",
                            "rowcount": cursor.rowcount
                        })
                except Exception as e:
                    if connection:
                        connection.rollback()
                    raise e
                finally:
                    if connection:
                        mysql_close_connection(connection)

            except Exception as e:
                return jsonify({
                    "success": False,
                    "message": f"MySQL error: {str(e)}"
                }), 500

        else:
            return jsonify({"success": False, "message": "Unsupported database type."}), 400

    except Exception as e:
        if connection:
            mysql_close_connection(connection)
        return jsonify({
            "success": False,
            "message": f"Error executing query: {str(e)}"
        }), 500

if __name__ == "__main__":
    app.run(debug=True)
