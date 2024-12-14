# keyword_mapping.py

keyword_mapping = {
    # Basic operations
    "select": ["show", "display", "list", "get", "find", "search", "retrieve","fetch"],
    "insert": ["add", "create", "put"],
    "update": ["modify", "change", "edit", "alter"],
    "delete": ["remove", "drop", "eliminate"],
    "all records":["show all","everything","show complete table","list all"],

    # Aggregations
    "where":["where"],
    "count": ["count","total", "number of", "how many"],
    "average": ["average","avg", "mean", "average of"],
    "sum": ["sum","total of", "sum of", "add up"],
    "maximum": ["maximum","max", "highest", "largest", "greatest"],
    "minimum": ["minimum","min", "lowest", "smallest", "least"],

    # Conditions
    "where": ["with", "having", "that has", "which has", "filter"],
    "greater": ["more than", "higher than", "above", "greater than", ">","is greater than"],
    "less": ["lower than", "below", "less than", "<","is less than","lesser than"],
    "equals": ["equal to", "is", "matches", "="],
    "like": ["similar to", "contains", "including"],
    
    # Grouping and Sorting
    "group": ["group by", "grouped by", "categorize by"],
    "order": ["sort", "arrange", "ordered by","order"],
    "asc": ["ascending", "increasing", "up"],
    "desc": ["descending", "decreasing", "down"],

    # Joins
    "join": ["combine", "merge", "connect"],
    "inner": ["matching", "where both"],
    "left": ["including all from first", "keeping all from left"],
    "right": ["including all from second", "keeping all from right"],

    # Set operations
    "distinct": ["unique", "different", "distinct values", "distinct"],
    "union": ["combine", "merge", "join sets"],
    "intersect": ["common", "shared", "both"],
    
    # Limit and Offset
    "limit": ["show only", "first", "top"],
    "offset": ["skip", "start from"],

    # Nulls
    "null": ["empty", "missing", "blank", "null value"],
    "not null": ["has value", "not empty", "not missing", "not blank"]
}