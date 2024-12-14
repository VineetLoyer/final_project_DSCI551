// Query Constructs for MySQL and MongoDB
const queryConstructs = {
    'mysql': ["GROUP BY", "WHERE", "ORDER BY", "DISTINCT"],
    'mongodb': ["GROUP BY", "WHERE", "ORDER BY", "DISTINCT"]
};

// Dynamically update construct selector based on database type
function updateConstructSelector() {
    const dbType = document.getElementById('db-type').value;
    const constructSelect = document.getElementById('sql-construct');
    constructSelect.innerHTML = queryConstructs[dbType]
        .map(construct => `<option value="${construct}">${construct}</option>`)
        .join('');
}

// Initialize event listeners
document.addEventListener('DOMContentLoaded', function() {
    const dbSelect = document.getElementById('db-type')
    dbSelect.value='mysql'
    updateConstructSelector();
    document.getElementById('db-type').addEventListener('change', updateConstructSelector);
    setupEventListeners();
    const event = new Event('change');
    dbSelect.dispatchEvent(event);
});

// Setup all event listeners
function setupEventListeners() {
    // Upload Dataset
    document.getElementById("upload-form").addEventListener("submit", handleUpload);
    
    // Database type change
    document.getElementById("db-type").addEventListener("change", loadTables);
    
    // Load Schema button
    document.getElementById("load-schema").addEventListener("click", loadSchema);
    
    // Load Preview button
    document.getElementById("load-preview").addEventListener("click", loadPreview);
    
    // Generate Queries button
    document.getElementById("generate-construct-queries").addEventListener("click", generateQueries);
    
    // Execute Query button
    document.getElementById("execute-query").addEventListener("click", executeQuery);
    
    // Natural Language Query button
    document.getElementById("nl-query-submit").addEventListener("click", handleNLQuery);
}

// Upload Dataset Handler
async function handleUpload(event) {
    event.preventDefault();
    const dataset = document.getElementById("dataset").files[0];
    const tableName = document.getElementById("table-name").value;

    if (!dataset || !tableName) {
        alert("Please select a file and enter a table name.");
        return;
    }

    const formData = new FormData();
    formData.append("dataset", dataset);
    formData.append("table_name", tableName);

    try {
        const response = await fetch("/upload-dataset", {
            method: "POST",
            body: formData,
        });

        const result = await response.json();
        showMessage(result.message, response.ok);
    } catch (error) {
        showMessage("Network error occurred.", false);
    }
}

// Load Tables/Collections
async function loadTables() {
    const dbType = this.value;
    const dropdown = document.getElementById("table-dropdown");
    dropdown.innerHTML = "";
    showMessage("Loading tables/collections...");

    try {
        const endpoint = dbType === "mysql" ? "mysql/tables" : "mongodb/collections";
        const response = await fetch(`/${endpoint}`);
        const data = await response.json();

        if (data.tables || data.collections) {
            const list = data.tables || data.collections;
            list.forEach(item => {
                const option = document.createElement("option");
                option.value = item;
                option.textContent = item;
                dropdown.appendChild(option);
            });
            showMessage("Tables/collections loaded successfully.", true);
        } else {
            showMessage("No tables or collections found.", false);
        }
    } catch (error) {
        console.error("Error:", error);
        showMessage("Error loading tables/collections.", false);
    }
}

// Load Schema
async function loadSchema() {
    const dbType = document.getElementById("db-type").value;
    const tableName = document.getElementById("table-dropdown").value;

    if (!tableName) {
        alert("Please select a table/collection.");
        return;
    }

    try {
        const response = await fetch(`/${dbType}/table/schema`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ table_name: tableName, collection_name: tableName }),
        });

        const data = await response.json();
        document.getElementById("schema-output").textContent = JSON.stringify(data.schema || data, null, 2);
    } catch (error) {
        console.error("Error:", error);
        showMessage("Error loading schema.", false);
    }
}

// Load Preview
async function loadPreview() {
    const dbType = document.getElementById("db-type").value;
    const tableName = document.getElementById("table-dropdown").value;

    if (!tableName) {
        alert("Please select a table/collection.");
        return;
    }

    try {
        const response = await fetch(`/${dbType}/table/preview`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ table_name: tableName, collection_name: tableName }),
        });

        const data = await response.json();
        renderPreview(data, dbType);
    } catch (error) {
        console.error("Error:", error);
    }
}

// Generate Queries
async function generateQueries() {
    const dbType = document.getElementById("db-type").value;
    const tableName = document.getElementById("table-dropdown").value;
    const schemaText = document.getElementById("schema-output").textContent;
    const selectedConstruct = document.getElementById("sql-construct").value;
    const queriesContainer = document.getElementById("sample-queries-container");

    queriesContainer.innerHTML = '';

    try {
        if (!tableName || !schemaText) {
            showError("Please select a table and load its schema first.");
            return;
        }

        const schema = JSON.parse(schemaText);
        
        const response = await fetch("/construct-queries", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
                table_name: tableName,
                schema: schema,
                construct: selectedConstruct,
                db_type: dbType,
            }),
        });

        const data = await response.json();

        if (!response.ok) {
            showError(data.error || "Error generating queries");
            return;
        }

        if (!data.queries || data.queries.length === 0) {
            showError("No queries generated");
            return;
        }

        renderQueries(data.queries, selectedConstruct, dbType);

    } catch (error) {
        console.error("Error:", error);
        showError("Error processing query generation");
    }
}

// Execute Query
// async function executeQuery() {
//     const dbType = document.getElementById("db-type").value;
//     const queryInput = document.getElementById("query-input").value;
//     const tableName = document.getElementById("table-dropdown").value;

//     if (!queryInput || !tableName) {
//         alert("Please enter a query and select a table/collection.");
//         return;
//     }

//     try {
//         const response = await fetch("/execute-query", {
//             method: "POST",
//             headers: { "Content-Type": "application/json" },
//             body: JSON.stringify({
//                 db_type: dbType,
//                 query: queryInput,
//                 table_name: tableName,
//             }),
//         });

//         const data = await response.json();
//         if (data.columns && data.rows) {
//             renderQueryResults(data.columns, data.rows);
//         } else {
//             document.getElementById("query-results").innerText = data.message || "No results found.";
//         }
//     } catch (error) {
//         console.error("Error:", error);
//         document.getElementById("query-results").innerText = "Error executing query.";
//     }
// }
// Execute Query
// Execute Query Function
async function executeQuery() {
    const dbType = document.getElementById("db-type").value;
    const queryInput = document.getElementById("query-input").value;
    const tableName = document.getElementById("table-dropdown").value;
    const resultsContainer = document.getElementById("query-results");

    if (!queryInput || !tableName) {
        alert("Please enter a query and select a table/collection.");
        return;
    }

    try {
        // Parse the query for MongoDB
        let query = queryInput;
        let operation="find";
        if (dbType === 'mongodb') {
            try {
                query = JSON.parse(queryInput);
                if (Array.isArray(query)) {
                    operation = "aggregate"; // Set operation to "aggregate" for pipelines
                }
            } catch (e) {
                alert("Invalid JSON query format");
                return;
            }
        }


        // Execute query
        const response = await fetch("/execute-query", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
                db_type: dbType,
                query: query,
                table_name: tableName,
                operation:operation
            })
        });

        const data = await response.json();
        
        // Clear previous results
        resultsContainer.innerHTML = '';

        // Show execution status
        const statusDiv = document.createElement('div');
        statusDiv.className = data.success ? 'alert alert-success' : 'alert alert-danger';
        statusDiv.textContent = data.message;
        resultsContainer.appendChild(statusDiv);

        if (data.success) {
            if (dbType === 'mysql' && data.columns && data.rows) {
                // Create table for MySQL results
                const tableDiv = document.createElement('div');
                tableDiv.className = 'table-responsive mt-3';
                tableDiv.innerHTML = `
                    <table class="table table-bordered table-hover">
                        <thead class="table-light">
                            <tr>
                                ${data.columns.map(col => `<th>${col}</th>`).join('')}
                            </tr>
                        </thead>
                        <tbody>
                            ${data.rows.map(row => `
                                <tr>
                                    ${row.map(cell => `<td>${cell !== null ? cell : 'NULL'}</td>`).join('')}
                                </tr>
                            `).join('')}
                        </tbody>
                    </table>
                `;
                resultsContainer.appendChild(tableDiv);
            } 
            else if (dbType === 'mongodb' && data.results) {
                // Create results display for MongoDB
                const resultsDiv = document.createElement('div');
                resultsDiv.className = 'mongodb-results mt-3';
                
                if (data.results.length === 0) {
                    resultsDiv.innerHTML = '<div class="alert alert-info">No documents found</div>';
                } else {
                    data.results.forEach((doc, index) => {
                        const docDiv = document.createElement('div');
                        docDiv.className = 'card mb-2';
                        docDiv.innerHTML = `
                            <div class="card-header">Document ${index + 1}</div>
                            <div class="card-body">
                                <pre class="mb-0"><code>${JSON.stringify(doc, null, 2)}</code></pre>
                            </div>
                        `;
                        resultsDiv.appendChild(docDiv);
                    });
                }
                resultsContainer.appendChild(resultsDiv);
            }
        }

        // Add styling
        const style = document.createElement('style');
        style.textContent = `
            .alert {
                padding: 10px;
                margin-bottom: 15px;
                border-radius: 4px;
            }
            .alert-success {
                color: #155724;
                background-color: #d4edda;
                border: 1px solid #c3e6cb;
            }
            .alert-danger {
                color: #721c24;
                background-color: #f8d7da;
                border: 1px solid #f5c6cb;
            }
            .alert-info {
                color: #0c5460;
                background-color: #d1ecf1;
                border: 1px solid #bee5eb;
            }
            .table {
                width: 100%;
                margin-bottom: 1rem;
                background-color: white;
                border-collapse: collapse;
            }
            .table th,
            .table td {
                padding: 0.75rem;
                border: 1px solid #dee2e6;
            }
            .table thead th {
                background-color: #f8f9fa;
                border-bottom: 2px solid #dee2e6;
            }
            .table tbody tr:nth-of-type(odd) {
                background-color: rgba(0,0,0,.05);
            }
            .table tbody tr:hover {
                background-color: rgba(0,0,0,.075);
            }
            .card {
                border: 1px solid rgba(0,0,0,.125);
                border-radius: 4px;
                margin-bottom: 1rem;
            }
            .card-header {
                padding: 0.75rem 1.25rem;
                background-color: #f8f9fa;
                border-bottom: 1px solid rgba(0,0,0,.125);
            }
            .card-body {
                padding: 1.25rem;
            }
            pre {
                margin: 0;
                white-space: pre-wrap;
                word-wrap: break-word;
            }
        `;
        document.head.appendChild(style);

    } catch (error) {
        console.error("Error:", error);
        resultsContainer.innerHTML = `
            <div class="alert alert-danger">
                Error executing query: ${error.message}
            </div>
        `;
    }
}

// Add event listener for Execute button
document.getElementById("execute-query").addEventListener("click", executeQuery);

// Handle Natural Language Query
// Natural Language Query Handler
// Handle Natural Language Query
async function handleNLQuery() {
    const nlQuery = document.getElementById("nl-query").value;
    const dbType = document.getElementById("db-type").value;
    const tableName = document.getElementById("table-dropdown").value;
    const resultsContainer = document.getElementById("nl-query-results");

    if (!nlQuery || !tableName) {
        alert("Please enter a natural language query and select a table/collection.");
        return;
    }

    try {
        const response = await fetch("/nl-query", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
                db_type: dbType,
                nl_query: nlQuery,
                table_name: tableName
            })
        });

        const data = await response.json();
        
        // Clear previous results
        resultsContainer.innerHTML = '';

        // Status message
        const statusDiv = document.createElement('div');
        statusDiv.className = data.success ? 'alert alert-success' : 'alert alert-danger';
        statusDiv.textContent = data.message;
        resultsContainer.appendChild(statusDiv);

        if (data.success) {
            // Create section for generated query
            const querySection = document.createElement('div');
            querySection.className = 'mt-3';

            // Add header for generated query
            const queryHeader = document.createElement('h5');
            queryHeader.textContent = `Generated ${dbType.toUpperCase()} Query:`;
            querySection.appendChild(queryHeader);

            // Add the query content
            const queryContent = document.createElement('div');
            queryContent.className = 'p-3 bg-light border rounded';
            
            // Format query based on database type
            let formattedQuery;
            if (dbType === 'mysql') {
                formattedQuery = data.query;
            } else {
                formattedQuery = JSON.stringify(data.query, null, 2);
            }
            
            queryContent.innerHTML = `<pre><code>${formattedQuery}</code></pre>`;
            querySection.appendChild(queryContent);

            // Add execute button
            const executeBtn = document.createElement('button');
            executeBtn.className = 'btn btn-primary mt-2';
            executeBtn.textContent = 'Execute Generated Query';
            executeBtn.onclick = () => executeGeneratedQuery(data.query, dbType);
            querySection.appendChild(executeBtn);

            resultsContainer.appendChild(querySection);
        }

        // Add styling
        const style = document.createElement('style');
        style.textContent = `
            .alert {
                padding: 10px;
                margin-bottom: 15px;
                border-radius: 4px;
            }
            .alert-success {
                color: #155724;
                background-color: #d4edda;
                border: 1px solid #c3e6cb;
            }
            .alert-danger {
                color: #721c24;
                background-color: #f8d7da;
                border: 1px solid #f5c6cb;
            }
            .bg-light {
                background-color: #f8f9fa;
            }
            .border {
                border: 1px solid #dee2e6;
            }
            .rounded {
                border-radius: 4px;
            }
            pre {
                margin: 0;
                white-space: pre-wrap;
                word-wrap: break-word;
            }
            .btn {
                display: inline-block;
                font-weight: 400;
                text-align: center;
                vertical-align: middle;
                cursor: pointer;
                padding: .375rem .75rem;
                font-size: 1rem;
                line-height: 1.5;
                border-radius: .25rem;
            }
            .btn-primary {
                color: #fff;
                background-color: #007bff;
                border-color: #007bff;
            }
            .mt-2 { margin-top: 0.5rem; }
            .mt-3 { margin-top: 1rem; }
            .p-3 { padding: 1rem; }
        `;
        document.head.appendChild(style);

    } catch (error) {
        console.error("Error:", error);
        resultsContainer.innerHTML = `
            <div class="alert alert-danger">
                Error processing natural language query: ${error.message}
            </div>
        `;
    }
}

// Execute generated query
async function executeGeneratedQuery(query, dbType) {
    // Set the query in the query input field
    const queryInput = document.getElementById("query-input");
    if (dbType === 'mysql') {
        queryInput.value = query;
    } else {
        queryInput.value = JSON.stringify(query, null, 2);
    }

    // Trigger the execute query function
    executeQuery();
}

// Add event listener for NL Query submit button
document.getElementById("nl-query-submit").addEventListener("click", handleNLQuery);
// Render Functions
function renderPreview(data, dbType) {
    const previewHeader = document.getElementById("preview-header");
    const previewBody = document.getElementById("preview-body");
    previewHeader.innerHTML = "";
    previewBody.innerHTML = "";

    if (dbType === "mysql" && data.columns && data.rows) {
        const headerRow = document.createElement("tr");
        data.columns.forEach(col => {
            const th = document.createElement("th");
            th.textContent = col;
            headerRow.appendChild(th);
        });
        previewHeader.appendChild(headerRow);

        data.rows.forEach(row => {
            const tr = document.createElement("tr");
            row.forEach(cell => {
                const td = document.createElement("td");
                td.textContent = cell;
                tr.appendChild(td);
            });
            previewBody.appendChild(tr);
        });
    } else if (dbType === "mongodb" && data.sample_data) {
        data.sample_data.forEach(doc => {
            const tr = document.createElement("tr");
            const td = document.createElement("td");
            td.textContent = JSON.stringify(doc, null, 2);
            td.colSpan = 3;
            tr.appendChild(td);
            previewBody.appendChild(tr);
        });
    }
}

function renderQueries(queries, construct, dbType) {
    const container = document.getElementById("sample-queries-container");
    
    container.innerHTML = `
        <style>
            .query-card {
                border: 1px solid #ddd;
                border-radius: 8px;
                padding: 15px;
                margin-bottom: 15px;
                background-color: white;
            }
            .query-header {
                margin-bottom: 10px;
                padding-bottom: 5px;
                border-bottom: 1px solid #eee;
            }
            .query-content {
                display: flex;
                flex-direction: column;
                gap: 10px;
            }
            .nl-query {
                background-color: #f8f9fa;
                padding: 10px;
                border-radius: 4px;
            }
            .query-section {
                position: relative;
            }
            .query-section pre {
                background-color: #f8f9fa;
                padding: 10px;
                border-radius: 4px;
                overflow-x: auto;
                margin: 5px 0;
            }
            .copy-btn {
                position: absolute;
                top: 5px;
                right: 5px;
                padding: 3px 8px;
                background-color: white;
                border: 1px solid #ddd;
                border-radius: 4px;
                cursor: pointer;
            }
            .copy-btn:hover {
                background-color: #f0f0f0;
            }
        </style>
        <h3>Sample Queries using ${construct}</h3>
        ${queries.map((query, index) => `
            <div class="query-card">
                <div class="query-header">
                    <h4>Example ${index + 1}: ${query.description}</h4>
                </div>
                <div class="query-content">
                    <div class="nl-query">
                        <strong>Natural Language:</strong>
                        <p>${query.nl_query}</p>
                    </div>
                    <div class="query-section">
                        <strong>${dbType.toUpperCase()} Query:</strong>
                        <pre><code>${formatQuery(query, dbType)}</code></pre>
                        <button class="copy-btn" onclick="copyQuery(this)">Copy Query</button>
                    </div>
                </div>
            </div>
        `).join('')}
    `;
}

function renderQueryResults(columns, rows) {
    const resultsContainer = document.getElementById("query-results");
    resultsContainer.innerHTML = "";

    const table = document.createElement("table");
    table.className = "query-results-table";

    // Create header
    const thead = document.createElement("thead");
    const headerRow = document.createElement("tr");
    columns.forEach(col => {
        const th = document.createElement("th");
        th.textContent = col;
        headerRow.appendChild(th);
    });
    thead.appendChild(headerRow);
    table.appendChild(thead);

    // Create body
    const tbody = document.createElement("tbody");
    rows.forEach(row => {
        const tr = document.createElement("tr");
        row.forEach(cell => {
            const td = document.createElement("td");
            td.textContent = cell;
            tr.appendChild(td);
        });
        tbody.appendChild(tr);
    });
    table.appendChild(tbody);

    resultsContainer.appendChild(table);
}

// Utility Functions
function showMessage(message, isSuccess = null) {
    const messageEl = document.getElementById("response-message");
    messageEl.innerText = message;
    if (isSuccess !== null) {
        messageEl.style.color = isSuccess ? "green" : "red";
    }
}

function showError(message) {
    const container = document.getElementById("sample-queries-container");
    container.innerHTML = `
        <div style="color: #dc3545; padding: 10px; background-color: #f8d7da; border: 1px solid #f5c6cb; border-radius: 4px;">
            ${message}
        </div>
    `;
}

function formatQuery(query, dbType) {
    if (dbType === 'mysql') {
        return query.sql;
    }
    return JSON.stringify(query.mongodb, null, 2);
}

function copyQuery(button) {
    const queryText = button.previousElementSibling.textContent;
    navigator.clipboard.writeText(queryText)
        .then(() => {
            const originalText = button.textContent;
            button.textContent = 'Copied!';
            setTimeout(() => {
                button.textContent = originalText;
            }, 2000);
        })
        .catch(err => {
            console.error('Failed to copy:', err);
            button.textContent = 'Failed to copy';
        });
}