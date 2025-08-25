# Mastering Data Analysis with the Databricks Genie API

This document provides a comprehensive overview of Databricks Genie, focusing on its core capabilities and how to leverage its official Conversation API to build powerful, automated data solutions.

Genie is the **conversational AI** integrated within the Databricks platform. Its primary role is to act as a data expert you can chat with, translating natural language prompts into executable code, explanations, and visualizations.

---

## Core Capabilities of Genie

Genie empowers data teams by simplifying complex tasks and accelerating the path from question to insight.

### Databricks AI/BI Genie Overview

Databricks AI/BI Genie is an AI-powered feature within the Databricks platform designed to enable natural language interaction with organizational data for business intelligence and data exploration.  

**Key aspects of Databricks AI/BI Genie:**

- **Natural Language Querying:**  
  Genie allows users to ask questions about their data using everyday language, eliminating the need for advanced technical skills like SQL.

- **Generative AI:**  
  It leverages generative AI models to translate natural language questions into analytical queries (e.g., SQL) and generate relevant responses, including data visualizations.

- **Contextual Understanding:**  
  Genie utilizes metadata from Unity Catalog, user-provided instructions, and sample queries to understand the specific terminology, business logic, and relationships within an organization's data.

- **Conversational Interface:**  
  It provides a conversational interface, allowing users to ask follow-up questions and refine their queries based on previous interactions.

- **Self-Service Analytics:**  
  Genie aims to empower business users to perform self-service data exploration and answer ad-hoc questions that might not be covered by existing dashboards or reports.

- **Continuous Improvement:**  
  It incorporates user feedback and clarifications to continuously refine its understanding of the data and improve the accuracy and relevance of its responses over time.

- **Integration with Dashboards:**  
  Genie can be embedded within Databricks AI/BI Dashboards, providing a seamless experience for asking follow-up questions directly within visualizations.

- **Genie Spaces:**  
  Genie operates within "Genie spaces," which are configurable environments where data analysts can define datasets, provide sample queries, and add instructions to guide Genie's performance and ensure accurate responses.

---

### 1. Natural Language to Code (SQL & Python)

This is Genie's cornerstone feature. It understands the context of your data (schemas, tables, columns) to generate high-quality code.

> **User Prompt:** "Show me the monthly revenue growth for our top 5 products in 2024. Also include the total number of unique customers for each product."

Genie translates this directly into an executable SQL query or a PySpark DataFrame operation, saving significant development time.

### 2. Automated Visualization

After generating a dataset, you can conversationally ask Genie to visualize the results.

> **User Prompt:** "Visualize this data as a stacked bar chart, with months on the x-axis and products stacked by revenue."

Genie will automatically generate the corresponding chart within the notebook, bypassing the need for manual configuration.

---

## Building an Agentic Framework with the Genie API

An agentic framework uses an AI agent to reason and execute a series of tasks. The official Genie API is the reasoning engine for this.

The agent operates in a loop: **Plan -> Act -> Observe -> Repeat**

- **Plan:** The agent breaks down a high-level goal (for example, "Analyze last quarter's sales performance") into a specific question. It then constructs the JSON payload for the Genie API, including the prompt, warehouse_id, and other context.  

- **Act:** The agent sends the POST request to the `/api/2.0/genie/conversation` endpoint. It parses the JSON response to extract the generated SQL or Python code from the `reply.content` field.  

- **Observe:** The agent executes the extracted code against the Databricks environment (for example, using the Databricks SQL Connector). The result of the execution (data or an error) becomes the observation.  

- **Repeat:** Based on the observation, the agent plans its next step. If the first query returned a list of top products, its next plan might be to generate a new query to analyze the sales trend for each of those products, using the same `session_id` to maintain context. This loop continues until the high-level goal is achieved.

---

## The Genie Conversation API: Automation and Integration

While the Genie UI is powerful for interactive analysis, its true potential for automation is unlocked via the official Conversation API. This API allows you to programmatically access the same powerful models that drive the conversational experience.

### API Endpoint and Authentication

- Method: `POST`
- Endpoint URL: `https://<your-databricks-workspace>/api/2.0/genie/conversation`
- Authentication: Requests are authenticated using a Databricks Personal Access Token (PAT) passed in the `Authorization: Bearer <token>` header.

### Key Request Parameters

- `session_id`: (Optional) A unique identifier to maintain conversation history.
- `prompt`: (Required) The natural language question or command for Genie.
- `warehouse_id`: (Required) The ID of the SQL warehouse to use as context for schemas and tables.
- `catalog_name`: (Optional) The name of the catalog to use for context.
- `schema_name`: (Optional) The name of the schema to use for context.

---
### Flow diagram
<img width="1652" height="1276" alt="image" src="https://github.com/user-attachments/assets/699c77e6-9d5b-45f5-97dd-df6348dcef9d" />

### API Request and Response Samples

Objective: Get the top 5 customers by total spending from a `sales` table located in the `main.retail` schema.

#### 1. API Request Payload
```
json
{
  "prompt": "Write a SQL query to find the top 5 customers by total spending.",
  "warehouse_id": "1234567890abcdef",
  "catalog_name": "main",
  "schema_name": "retail",
  "session_id": "user-session-42"
}

### cURL Example

This shows how to make the API call from the command line, including the authentication header.

bash
curl --request POST \
  --url https://<your-databricks-workspace>/api/2.0/genie/conversation \
  --header "Authorization: Bearer $DATABRICKS_TOKEN" \
  --header "Content-Type: application/json" \
  --data '{
    "prompt": "Write a SQL query to find the top 5 customers by total spending.",
    "warehouse_id": "1234567890abcdef",
    "catalog_name": "main",
    "schema_name": "retail",
    "session_id": "user-session-42"
  }
The API returns a JSON object. The generated code and explanation are found within the reply object.
{
  "session_id": "user-session-42",
  "reply": {
    "type": "sql",
    "content": "SELECT\n  customer_id,\n  SUM(quantity * price_per_unit) AS total_spending\nFROM\n  main.retail.sales\nGROUP BY\n  customer_id\nORDER BY\n  total_spending DESC\nLIMIT 5;",
    "explanation": "This SQL query calculates the total spending for each customer by multiplying the quantity by the price per unit. It then groups the results by customer, orders the results by total spending in descending order, and returns the top 5 customers."
  },
  "status": {
    "state": "SUCCESS"
  }
}





