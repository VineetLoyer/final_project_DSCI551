# ChatDB

ChatDB is a full-stack application that simplifies database interaction. It allows users to upload datasets, explore database schemas, execute complex queries, and even generate queries from natural language inputs. The project supports both MySQL and MongoDB, offering a seamless experience for working with structured and unstructured data.

---

## Features

1. **Upload Dataset**
   - Upload datasets in CSV or JSON format.
   - Automatically saves data into MySQL tables or MongoDB collections.

2. **Explore Database**
   - View database schemas and preview table or collection data.
   - Supports dynamic exploration for both MySQL and MongoDB.

3. **Execute Queries**
   - Execute SQL or MongoDB queries and visualize results dynamically.
   - Queries can be input manually or generated from templates.

4. **Natural Language Query (NLQ) Support**
   - Converts user-friendly text into structured SQL or MongoDB queries.
   - Powered by Natural Language Processing (NLP) using NLTK.

5. **Cross-Database Compatibility**
   - Supports both relational (MySQL) and NoSQL (MongoDB) databases.

---

## Tech Stack

### **Frontend**
- **HTML**, **CSS**, **JavaScript**
  - For building a responsive, user-friendly interface.

### **Backend**
- **Flask**
  - RESTful API for handling user requests and database operations.
- **Werkzeug**
  - For secure file uploads.
- **NLTK**
  - For natural language processing of queries.

### **Databases**
- **MySQL**
  - For structured, relational data.
- **MongoDB**
  - For unstructured, document-based data.

---

## Installation

### Prerequisites

- **Python 3.8+**
- **MySQL** installed and running
- **MongoDB** installed and running

### Steps

1. **Clone the Repository**
   ```bash
   git clone https://github.com/your-repo/chatdb.git
   cd chatdb
   ```

2. **Set Up Virtual Environment**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install Dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Configure Databases**
   - **MySQL**:
     - Create a database named `chatdb`.
     - Update `mysql_connection.py` with your MySQL credentials.
   - **MongoDB**:
     - Ensure MongoDB is running and accessible at `mongodb://localhost:27017/`.

5. **Run the Application**
   ```bash
   flask run
   ```

6. **Access the Application**
   - Open a browser and navigate to `http://127.0.0.1:5000/`.

---

## Usage

1. **Upload Dataset**
   - Navigate to the "Upload Dataset" section.
   - Select a CSV or JSON file and specify a table/collection name.
   - Click "Upload" to store the data in MySQL or MongoDB.

2. **Explore Database**
   - Select the database type (MySQL or MongoDB).
   - Choose a table/collection to view its schema and preview data.

3. **Execute Queries**
   - Enter an SQL or MongoDB query in the query box.
   - Click "Execute" to view results.

4. **Generate Natural Language Queries**
   - Type a natural language question (e.g., "Show all employees with a salary greater than 50000").
   - Click "Execute" to see the generated SQL/MongoDB query and results.

---

## Project Structure

```
chatdb/
├── backend/
│   ├── mysql_connection.py  # MySQL database connection
│   ├── mongodb_connection.py # MongoDB database connection
│   
├── frontend/
│   ├── index1.html          # Main HTML file
│   ├── style1.css           # Styling for the app
│   ├── app1.js              # JavaScript for dynamic interactions
│
├── app2_copy.py             # Main Flask application
├── query_constructs.py      # Query generation logic
├── keyword_mapping.py       # Mapping for natural language keywords
├── requirements.txt         # Project dependencies
```

---

## Key Learnings

1. **Full-Stack Development**
   - Built a complete application using Flask, MySQL, MongoDB, and a responsive frontend.

2. **Database Integration**
   - Learned how to manage structured (MySQL) and unstructured (MongoDB) data in one application.

3. **Natural Language Processing (NLP)**
   - Converted user-friendly text into structured database queries using NLTK.

4. **Security Best Practices**
   - Ensured secure file handling using Werkzeug.
   - Prevented SQL injection and other vulnerabilities by validating inputs.

5. **RESTful API Design**
   - Designed APIs for dataset uploads, schema exploration, query execution, and NLQ generation.

---

## Future Improvements

- Add support for more databases like PostgreSQL or SQLite.
- Implement complex query constructs like join and nested queries.
- Implement user authentication for better security.
- Provide detailed analytics, error logs, and query performance insights.

---

## License

This project is licensed under the MIT License. Feel free to use, modify, and distribute it as per the license terms.

