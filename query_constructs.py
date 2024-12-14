class QueryConstructs:
    @staticmethod
    def get_construct_templates():
        return {
            "SELECT": {
                "patterns": [
                    # Basic SELECT patterns
                    {
                        "description": "Single column",
                        "nl_template": "show|display|get|find {column}",
                        "sql_template": "SELECT {column} FROM {table};",
                        "mongodb_template": {"$project": {"{column}": 1, "_id": 0}}
                    },
                    {
                        "description": "Multiple columns",
                        "nl_template": "show|display|get {columns}",
                        "sql_template": "SELECT {columns} FROM {table};",
                        "mongodb_template": {"$project": {column: 1 for column in "{columns}".split(', ') if column}, "_id": 0}
                    },
                    {
                        "description": "All columns",
                        "nl_template": "show|display|get all|everything|show all",
                        "sql_template": "SELECT * FROM {table};",
                        "mongodb_template": {}
                    },

                    # SELECT with WHERE conditions
                    {
                        "description": "Equals condition",
                        "nl_template": "find|show records|rows where {column} equals|is {value}",
                        "sql_template": "SELECT * FROM {table} WHERE {column} = '{value}';",
                        "mongodb_template": {"$match": {"{column}": "{value}"}}
                    },
                    {
                        "description": "Greater than condition",
                        "nl_template": "find|show records|rows where {column} greater|more than {value}",
                        "sql_template": "SELECT * FROM {table} WHERE {column} > {value};",
                        "mongodb_template": {"$match": {"{column}": {"$gt": "{value}"}}}
                    },
                    {
                        "description": "Less than condition",
                        "nl_template": "show {column} where|with {condition_col} less|lower|smaller than {value}",
                        "sql_template": "SELECT {column} FROM {table} WHERE {condition_col} < {value};",
                        "mongodb_template": {"$match": {"{condition_col}": {"$lt": "{value}"}}}
                    },
                    {
                        "description": "Like condition",
                        "nl_template": "show {column} where|with {condition_col} contains|like {value}",
                        "sql_template": "SELECT {column} FROM {table} WHERE {condition_col} LIKE '%{value}%';",
                        "mongodb_template": {"$match": {"{condition_col}": {"$regex": "{value}", "$options": "i"}}}
                    },

                    # Multiple conditions
                    {
                        "description": "Multiple AND conditions",
                        "nl_template": "show {column} where {condition_col1} equals {value1} and {condition_col2} greater than {value2}",
                        "sql_template": "SELECT {column} FROM {table} WHERE {condition_col1} = '{value1}' AND {condition_col2} > {value2};",
                        "mongodb_template": {
                            "$match": {
                                "{condition_col1}": "{value1}",
                                "{condition_col2}": {"$gt": "{value2}"}
                            }
                        }
                    },
                    {
                        "description": "Multiple OR conditions",
                        "nl_template": "show {column} where {condition_col1} equals {value1} or {condition_col2} less than {value2}",
                        "sql_template": "SELECT {column} FROM {table} WHERE {condition_col1} = '{value1}' OR {condition_col2} < {value2};",
                        "mongodb_template": {
                            "$match": {
                                "$or": [
                                    {"{condition_col1}": "{value1}"},
                                    {"{condition_col2}": {"$lt": "{value2}"}}
                                ]
                            }
                        }
                    }
                ]
            },

            "AGGREGATE": {
                "patterns": [
                    # Basic aggregates
                    {
                        "description": "Simple average",
                        "nl_template": "show|get|calculate average|mean|avg of {column}",
                        "sql_template": "SELECT AVG({column}) as avg_{column} FROM {table};",
                        "mongodb_template": [{"$group": {"_id": None, "avg_{column}": {"$avg": "${column}"}}}]
                    },
                    {
                        "description": "Simple sum",
                        "nl_template": "show|get|calculate sum|total of {column}",
                        "sql_template": "SELECT SUM({column}) as total_{column} FROM {table};",
                        "mongodb_template": [{"$group": {"_id": None, "total_{column}": {"$sum": "${column}"}}}]
                    },
                    {
                        "description": "Simple count",
                        "nl_template": "count total|all records|rows",
                        "sql_template": "SELECT COUNT(*) as total_count FROM {table};",
                        "mongodb_template": [{"$group": {"_id": None, "count": {"$sum": 1}}}]
                    },
                    {
                        "description": "Min and Max",
                        "nl_template": "show|get minimum and maximum|min and max of {column}",
                        "sql_template": "SELECT MIN({column}) as min_{column}, MAX({column}) as max_{column} FROM {table};",
                        "mongodb_template": [
                            {
                                "$group": {
                                    "_id": None,
                                    "min_{column}": {"$min": "${column}"},
                                    "max_{column}": {"$max": "${column}"}
                                }
                            }
                        ]
                    },

                    # Aggregates with conditions
                    {
                        "description": "Average with condition",
                        "nl_template": "show average of {column} where {condition_col} greater than {value}",
                        "sql_template": "SELECT AVG({column}) FROM {table} WHERE {condition_col} > {value};",
                        "mongodb_template": [
                            {"$match": {"{condition_col}": {"$gt": "{value}"}}},
                            {"$group": {"_id": None, "average": {"$avg": "${column}"}}}
                        ]
                    },
                    {
                        "description": "Sum with condition",
                        "nl_template": "show sum of {column} where {condition_col} equals {value}",
                        "sql_template": "SELECT SUM({column}) FROM {table} WHERE {condition_col} = '{value}';",
                        "mongodb_template": [
                            {"$match": {"{condition_col}": "{value}"}},
                            {"$group": {"_id": None, "sum": {"$sum": "${column}"}}}
                        ]
                    },

                    # Multiple aggregates
                    {
                        "description": "Multiple aggregates same column",
                        "nl_template": "show average and sum of {column}",
                        "sql_template": "SELECT AVG({column}) as avg_{column}, SUM({column}) as sum_{column} FROM {table};",
                        "mongodb_template": [
                            {
                                "$group": {
                                    "_id": None,
                                    "avg_{column}": {"$avg": "${column}"},
                                    "sum_{column}": {"$sum": "${column}"}
                                }
                            }
                        ]
                    },
                    {
                        "description": "Multiple aggregates different columns",
                        "nl_template": "show average of {column1} and sum of {column2}",
                        "sql_template": "SELECT AVG({column1}) as avg_{column1}, SUM({column2}) as sum_{column2} FROM {table};",
                        "mongodb_template": [
                            {
                                "$group": {
                                    "_id": None,
                                    "avg_{column1}": {"$avg": "${column1}"},
                                    "sum_{column2}": {"$sum": "${column2}"}
                                }
                            }
                        ]
                    }
                ]
            },
            "WHERE": {  # Changed from "where" to "WHERE"
                "patterns": [
                    {
                        "description": "Equals condition",
                        "nl_template": "Find records where {column} equals {value}",
                        "sql_template": "SELECT * FROM {table} WHERE {column} = '{value}';",
                        "mongodb_template": {
                            "$match": {"{column}": "{value}"}
                        },
                        "value_type": "text"
                    },
                    {
                        "description": "Greater than condition",
                        "nl_template": "Find records where {column} is greater than {value}",
                        "sql_template": "SELECT * FROM {table} WHERE {column} > {value};",
                        "mongodb_template": {
                            "$match": {"{column}": {"$gt": "{value}"}}
                        },
                        "value_type": "numeric"
                    },
                    {
                        "description": "Less than condition",
                        "nl_template": "Find records where {column} is less than {value}",
                        "sql_template": "SELECT * FROM {table} WHERE {column} < {value};",
                        "mongodb_template": {
                            "$match": {"{column}": {"$lt": "{value}"}}
                        },
                        "value_type": "numeric"
                    },
                    {
                        "description": "Multiple conditions (AND)",
                        "nl_template": "Find records where {column1} equals {value1} and {column2} greater than {value2}",
                        "sql_template": "SELECT * FROM {table} WHERE {column1} = '{value1}' AND {column2} > {value2};",
                        "mongodb_template": {
                            "$match": {
                                "{column1}": "{value1}",
                                "{column2}": {"$gt": "{value2}"}
                            }
                        },
                        "value_type": "mixed"
                    },
                    {
                        "description": "Multiple conditions (OR)",
                        "nl_template": "Find records where {column1} equals {value1} or {column2} less than {value2}",
                        "sql_template": "SELECT * FROM {table} WHERE {column1} = '{value1}' OR {column2} < {value2};",
                        "mongodb_template": {
                            "$match": {
                                "$or": [
                                    {"{column1}": "{value1}"},
                                    {"{column2}": {"$lt": "{value2}"}}
                                ]
                            }
                        },
                        "value_type": "mixed"
                    }
                ]
            },
            "GROUP BY": {
                "patterns": [
                    {
                        "description": "Simple group by count",
                        "nl_template": "count records|rows grouped by {column}",
                        "sql_template": "SELECT {column}, COUNT(*) as count FROM {table} GROUP BY {column};",
                        "mongodb_template": [
                            {
                                "$group": {
                                    "_id": "${column}",
                                    "count": {"$sum": 1},
                                    "{column}": {"$first": "${column}"}
                                }
                            },
                            {
                                "$project": {
                                    "_id": 0,
                                    "{column}": 1,
                                    "count": 1
                                }
                            }
                        ],
                        "value_type": "text"  # Added value_type
                    },
                    {
                        "description": "Group by with sum",
                        "nl_template": "show|calculate sum of {column1} grouped by {column2}",
                        "sql_template": "SELECT {column2}, SUM({column1}) as total FROM {table} GROUP BY {column2};",
                        "mongodb_template": [
                            {
                                "$group": {
                                    "_id": "${column2}",
                                    "total": {"$sum": "${column1}"},
                                    "{column2}": {"$first": "${column2}"}
                                }
                            },
                            {
                                "$project": {
                                    "_id": 0,
                                    "{column2}": 1,
                                    "total": 1
                                }
                            }
                        ],
                        "value_type": "numeric"  # Added value_type
                    },
                    {
                        "description": "Group by with having",
                        "nl_template": "show {column1} grouped by {column2} having count greater than {value}",
                        "sql_template": """
                            SELECT {column2}, COUNT(*) as count 
                            FROM {table} 
                            GROUP BY {column2} 
                            HAVING COUNT(*) > {value};
                        """,
                        "mongodb_template": [
                            {
                                "$group": {
                                    "_id": "${column2}",
                                    "count": {"$sum": 1},
                                    "{column2}": {"$first": "${column2}"}
                                }
                            },
                            {
                                "$match": {
                                    "count": {"$gt": "{value}"}
                                }
                            },
                            {
                                "$project": {
                                    "_id": 0,
                                    "{column2}": 1,
                                    "count": 1
                                }
                            }
                        ],
                        "value_type": "numeric"  # Added value_type
                    }
                ]
            },

            "ORDER BY": {
                 "patterns": [
                    {
                        "description": "Sort by single column ascending",
                        "nl_template": "sort|arrange|order records|rows by {column} ascending|asc",
                        "sql_template": "SELECT * FROM {table} ORDER BY {column} ASC;",
                        "mongodb_template": [
                            {"$sort": {"{column}": 1}},
                            {
                                "$project": {
                                    "_id": 0,
                                    "{column}": 1,
                                    "*": "$$ROOT"
                                }
                            }
                        ],
                        "value_type": "text"  # Added value_type
                    },
                    {
                        "description": "Sort by single column descending",
                        "nl_template": "sort|arrange|order records|rows by {column} descending|desc",
                        "sql_template": "SELECT * FROM {table} ORDER BY {column} DESC;",
                        "mongodb_template": [{"$sort": {"{column}": -1}}],
                        "value_type": "text"  # Added value_type
                    },
                    {
                        "description": "Sort by single column (default ascending)",
                        "nl_template": "sort|arrange|order records|rows by {column}",
                        "sql_template": "SELECT * FROM {table} ORDER BY {column};",
                        "mongodb_template": [
                            {"$sort": {"{column}": 1}},
                            {
                                "$project": {
                                    "_id": 0,
                                    "{column}": 1,
                                    "*": "$$ROOT"
                                }
                            }
                        ],
                        "value_type": "text"  # Added value_type
                    },
                    {
                        "description": "Sort by multiple columns",
                        "nl_template": "sort|arrange|order records|rows by {column1} ascending|asc and {column2} descending|desc",
                        "sql_template": "SELECT * FROM {table} ORDER BY {column1} ASC, {column2} DESC;",
                        "mongodb_template": [
                            {
                                "$sort": {
                                    "{column1}": 1,
                                    "{column2}": -1
                                }
                            },
                            {
                                "$project": {
                                    "_id": 0,
                                    "{column1}": 1,
                                    "{column2}": 1,
                                    "*": "$$ROOT"
                                }
                            }
                        ],
                        "value_type": "mixed"  # Added value_type
                    }
                ]
            },

            "DISTINCT": {
                 "patterns": [
                    {
                        "description": "Simple distinct",
                        "nl_template": "show unique|distinct values of {column}",
                        "sql_template": "SELECT DISTINCT {column} FROM {table};",
                        "mongodb_template": [
                            {
                                "$group": {
                                    "_id": "${column}",
                                    "unique_value": {"$first": "${column}"}
                                }
                            },
                            {
                                "$project": {
                                    "_id": 0,
                                    "{column}": "$unique_value"
                                }
                            }
                        ],
                        "value_type": "text"  # Added value_type
                    },
                    {
                        "description": "Distinct with condition",
                        "nl_template": "show unique|distinct values of {column} where {condition_col} greater than {value}",
                        "sql_template": "SELECT DISTINCT {column} FROM {table} WHERE {condition_col} > {value};",
                        "mongodb_template": [
                            {
                                "$match": {
                                    "{condition_col}": {"$gt": "{value}"}
                                }
                            },
                            {
                                "$group": {
                                    "_id": "${column}",
                                    "unique_value": {"$first": "${column}"}
                                }
                            },
                            {
                                "$project": {
                                    "_id": 0,
                                    "{column}": "$unique_value"
                                }
                            }
                        ],
                        "value_type": "numeric"  # Added value_type
                    },
                    {
                        "description": "Multiple distinct columns",
                        "nl_template": "show unique|distinct combinations of {column1} and {column2}",
                        "sql_template": "SELECT DISTINCT {column1}, {column2} FROM {table};",
                        "mongodb_template": [
                            {
                                "$group": {
                                    "_id": {
                                        "col1": "${column1}",
                                        "col2": "${column2}"
                                    },
                                    "{column1}": {"$first": "${column1}"},
                                    "{column2}": {"$first": "${column2}"}
                                }
                            },
                            {
                                "$project": {
                                    "_id": 0,
                                    "{column1}": 1,
                                    "{column2}": 1
                                }
                            }
                        ],
                        "value_type": "mixed"  # Added value_type
                    }
                ]
            },
            "UPDATE": {
                "patterns": [
                    # Simple updates
                    {
                        "description": "Update single column",
                        "nl_template": "update|change|set {column} to {value} where {condition_col} equals {condition_value}",
                        "sql_template": "UPDATE {table} SET {column} = '{value}' WHERE {condition_col} = '{condition_value}';",
                        "mongodb_template": {
                            "update": True,
                            "filter": {"{condition_col}": "{condition_value}"},
                            "update": {"$set": {"{column}": "{value}"}}
                        }
                    },
                    {
                        "description": "Update multiple columns",
                        "nl_template": "update|change|set {column1} to {value1} and {column2} to {value2} where {condition_col} equals {condition_value}",
                        "sql_template": "UPDATE {table} SET {column1} = '{value1}', {column2} = '{value2}' WHERE {condition_col} = '{condition_value}';",
                        "mongodb_template": {
                            "update": True,
                            "filter": {"{condition_col}": "{condition_value}"},
                            "update": {
                                "$set": {
                                    "{column1}": "{value1}",
                                    "{column2}": "{value2}"
                                }
                            }
                        }
                    },

                    # Numeric updates
                    {
                        "description": "Increment value",
                        "nl_template": "increase|increment {column} by {value} where {condition_col} equals {condition_value}",
                        "sql_template": "UPDATE {table} SET {column} = {column} + {value} WHERE {condition_col} = '{condition_value}';",
                        "mongodb_template": {
                            "update": True,
                            "filter": {"{condition_col}": "{condition_value}"},
                            "update": {"$inc": {"{column}": "{value}"}}
                        }
                    },
                    {
                        "description": "Decrease value",
                        "nl_template": "decrease|decrement {column} by {value} where {condition_col} equals {condition_value}",
                        "sql_template": "UPDATE {table} SET {column} = {column} - {value} WHERE {condition_col} = '{condition_value}';",
                        "mongodb_template": {
                            "update": True,
                            "filter": {"{condition_col}": "{condition_value}"},
                            "update": {"$inc": {"{column}": "-{value}"}}
                        }
                    },

                    # Conditional updates
                    {
                        "description": "Update with multiple conditions",
                        "nl_template": "update {column} to {value} where {condition_col1} greater than {value1} and {condition_col2} less than {value2}",
                        "sql_template": "UPDATE {table} SET {column} = '{value}' WHERE {condition_col1} > {value1} AND {condition_col2} < {value2};",
                        "mongodb_template": {
                            "update": True,
                            "filter": {
                                "{condition_col1}": {"$gt": "{value1}"},
                                "{condition_col2}": {"$lt": "{value2}"}
                            },
                            "update": {"$set": {"{column}": "{value}"}}
                        }
                    },

                    # NULL updates
                    {
                        "description": "Set to NULL",
                        "nl_template": "set {column} to null|empty where {condition_col} equals {value}",
                        "sql_template": "UPDATE {table} SET {column} = NULL WHERE {condition_col} = '{value}';",
                        "mongodb_template": {
                            "update": True,
                            "filter": {"{condition_col}": "{value}"},
                            "update": {"$set": {"{column}": None}}
                        }
                    }
                ]
            },

            "INSERT": {
                "patterns": [
                    # Single row insert
                    {
                        "description": "Insert single value",
                        "nl_template": "insert|add|create record|row with {column} equals {value}",
                        "sql_template": "INSERT INTO {table} ({column}) VALUES ('{value}');",
                        "mongodb_template": {
                            "insert": True,
                            "document": {"{column}": "{value}"}
                        }
                    },
                    {
                        "description": "Insert multiple columns",
                        "nl_template": "insert|add|create record|row with {column1} equals {value1} and {column2} equals {value2}",
                        "sql_template": "INSERT INTO {table} ({column1}, {column2}) VALUES ('{value1}', '{value2}');",
                        "mongodb_template": {
                            "insert": True,
                            "document": {
                                "{column1}": "{value1}",
                                "{column2}": "{value2}"
                            }
                        }
                    },

                    # Multiple row insert
                    {
                        "description": "Insert multiple rows",
                        "nl_template": "insert multiple records|rows with {column} values {value1} and {value2}",
                        "sql_template": "INSERT INTO {table} ({column}) VALUES ('{value1}'), ('{value2}');",
                        "mongodb_template": {
                            "insertMany": True,
                            "documents": [
                                {"{column}": "{value1}"},
                                {"{column}": "{value2}"}
                            ]
                        }
                    },

                    # Insert with default values
                    {
                        "description": "Insert with defaults",
                        "nl_template": "insert record|row with {column} equals {value} and default values",
                        "sql_template": "INSERT INTO {table} ({column}) VALUES ('{value}');",
                        "mongodb_template": {
                            "insert": True,
                            "document": {"{column}": "{value}"}
                        }
                    },

                    # Copy data insert
                    {
                        "description": "Insert from select",
                        "nl_template": "insert records|rows from select where {condition_col} equals {value}",
                        "sql_template": "INSERT INTO {table} SELECT * FROM {source_table} WHERE {condition_col} = '{value}';",
                        "mongodb_template": [
                            {"$match": {"{condition_col}": "{value}"}},
                            {"$merge": {"into": "{table}"}}
                        ]
                    }
                ]
            },

            "DELETE": {
                "patterns": [
                    # Simple delete
                    {
                        "description": "Delete with condition",
                        "nl_template": "delete|remove records|rows where {column} equals {value}",
                        "sql_template": "DELETE FROM {table} WHERE {column} = '{value}';",
                        "mongodb_template": {
                            "delete": True,
                            "filter": {"{column}": "{value}"}
                        }
                    },

                    # Multiple conditions
                    {
                        "description": "Delete with multiple conditions",
                        "nl_template": "delete records where {column1} equals {value1} and {column2} greater than {value2}",
                        "sql_template": "DELETE FROM {table} WHERE {column1} = '{value1}' AND {column2} > {value2};",
                        "mongodb_template": {
                            "delete": True,
                            "filter": {
                                "{column1}": "{value1}",
                                "{column2}": {"$gt": "{value2}"}
                            }
                        }
                    },

                    # Delete with IN clause
                    {
                        "description": "Delete multiple values",
                        "nl_template": "delete records where {column} in {value1} {value2}",
                        "sql_template": "DELETE FROM {table} WHERE {column} IN ('{value1}', '{value2}');",
                        "mongodb_template": {
                            "delete": True,
                            "filter": {"{column}": {"$in": ["{value1}", "{value2}"]}}
                        }
                    },

                    # Delete NULL values
                    {
                        "description": "Delete null records",
                        "nl_template": "delete records where {column} is null|empty",
                        "sql_template": "DELETE FROM {table} WHERE {column} IS NULL;",
                        "mongodb_template": {
                            "delete": True,
                            "filter": {"{column}": None}
                        }
                    }
                ]
            }
        }
        
    
    @staticmethod
    def get_operation_keywords():
        """Keywords that indicate the operation type"""
        return {
            "SELECT": ["show", "display", "get", "find", "select", "list"],
            "UPDATE": ["update", "change", "modify", "set"],
            "INSERT": ["insert", "add", "create", "put"],
            "DELETE": ["delete", "remove", "drop"],
            "GROUP BY": ["group", "grouped", "combine"],
            "ORDER BY": ["sort", "order", "arrange"],
            "AGGREGATE": ["calculate", "compute", "total", "average", "mean"]
        }

    @staticmethod
    def get_condition_keywords():
        """Keywords for condition clauses"""
        return {
            "EQUALS": ["equals", "is", "matches", "="],
            "GREATER": ["greater than", "more than", "above", ">", ">="],
            "LESS": ["less than", "below", "under", "<", "<="],
            "NOT": ["not", "isn't", "doesn't", "!="],
            "LIKE": ["like", "contains", "similar to"],
            "NULL": ["null", "empty", "missing"],
            "IN": ["in", "within", "among"]
        }