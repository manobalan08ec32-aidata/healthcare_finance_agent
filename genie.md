# Mastering Data Analysis with the Databricks Genie API

This document provides a comprehensive overview of Databricks Genie, focusing on its core capabilities and how to leverage its functionality through the API to build powerful, automated data solutions.

Genie is the **conversational AI** integrated within the Databricks platform. Its primary role is to act as a data expert you can chat with, translating natural language prompts into executable code, explanations, and visualizations.

---

## Core Capabilities of Genie

Genie empowers data teams by simplifying complex tasks and accelerating the path from question to insight.

### 1. Natural Language to Code (SQL & Python)
This is Genie's cornerstone feature. It understands the context of your data (schemas, tables, columns) to generate high-quality code.

> **User Prompt:** "Show me the monthly revenue growth for our top 5 products in 2024. Also include the total number of unique customers for each product."

Genie translates this directly into an executable SQL query or a PySpark DataFrame operation, saving significant development time.

### 2. Code Explanation and Debugging
You can use Genie to understand or fix existing code. Paste a complex script and ask it to:
* **Explain this query:** Genie provides a step-by-step breakdown in plain English.
* **Fix the error in this code:** Genie analyzes the code, identifies syntax or logical errors, and suggests a corrected version.
* **Convert this Pandas code to PySpark:** Genie refactors code to make it scalable and optimized for the Databricks environment.

### 3. Automated Visualization
After generating a dataset, you can conversationally ask Genie to visualize the results.

> **User Prompt:** "Visualize this data as a stacked bar chart, with months on the x-axis and products stacked by revenue."

Genie will automatically generate the corresponding chart within the notebook, bypassing the need for manual configuration.

---

## The Genie API: Automation and Integration

While the Genie UI is powerful for interactive analysis, its true potential for automation is unlocked via its API. The functionality of Genie is exposed through the **Databricks Foundation Model APIs**, which allow you to programmatically access the same powerful models that drive the conversational experience.

### How it Works
You can build applications that send prompts (including context like table schemas) to a hosted large language model endpoint (e.g., DBRX Instruct). The model processes the request and returns a structured JSON object containing the generated code or explanation.

* **API Endpoint:** The API is served through Databricks Model Serving endpoints.
* **Authentication:** Requests are authenticated using Databricks Personal Access Tokens (PAT).
* **Context is Key:** For the best results, your API prompt must include schema information (e.g., `CREATE TABLE` statements or column descriptions) along with your natural language question.

### API Request and Response Samples

Here are practical examples of how to interact with the API that powers Genie's capabilities.

#### Example 1: Text-to-SQL Generation

**Objective:** Get the top 5 customers by total spending from a `sales` table.

**1. The Prompt (with Schema Context)**

To ensure accuracy, we provide the table schema within the prompt itself.

You are an expert SQL analyst. Given the following database schema, write a SQL query to find the top 5 customers by total spending.

Schema:
CREATE TABLE sales (
customer_id STRING,
product_id STRING,
sale_date DATE,
quantity INT,
price_per_unit DECIMAL(10, 2)
);


**2. API Request Payload**

The request is sent to the model serving endpoint. We use the `messages` format for conversational models.

```json
{
  "messages": [
    {
      "role": "user",
      "content": "You are an expert SQL analyst. Given the following database schema, write a SQL query to find the top 5 customers by total spending.\n\nSchema:\nCREATE TABLE sales (\n  customer_id STRING,\n  product_id STRING,\n  sale_date DATE,\n  quantity INT,\n  price_per_unit DECIMAL(10, 2)\n);"
    }
  ],
  "max_tokens": 1024
}
3. cURL Example

This shows how to make the API call from the command line.

Bash
curl -X POST -u "token:$DATABRICKS_TOKEN" \
  https://<your-databricks-workspace>.databricks.net/serving-endpoints/databricks-dbrx-instruct/invocations \
  -H "Content-Type: application/json" \
  -d '{
    "messages": [
      {
        "role": "user",
        "content": "You are an expert SQL analyst. Given the following database schema, write a SQL query to find the top 5 customers by total spending.\n\nSchema:\nCREATE TABLE sales (\n  customer_id STRING,\n  product_id STRING,\n  sale_date DATE,\n  quantity INT,\n  price_per_unit DECIMAL(10, 2)\n);"
      }
    ],
    "max_tokens": 1024
  }'
4. API Response

The API returns a JSON object containing the model's response. The generated SQL is in the content field.

JSON
{
  "choices": [
    {
      "index": 0,
      "message": {
        "role": "assistant",
        "content": "Of course. Here is the SQL query to find the top 5 customers by total spending:\n\n```sql\nSELECT\n  customer_id,\n  SUM(quantity * price_per_unit) AS total_spending\nFROM\n  sales\nGROUP BY\n  customer_id\nORDER BY\n  total_spending DESC\nLIMIT 5;\n```"
      }
    }
  ],
  "usage": {
    "prompt_tokens": 88,
    "completion_tokens": 76,
    "total_tokens": 164
  }
}
Building an Agentic Framework with the Genie API
An agentic framework uses an AI agent to reason and execute a series of tasks to achieve a goal. The Genie API is the perfect "reasoning engine" for this.

The agent operates in a loop: Plan -> Observe -> Act.

Plan: The agent breaks down a high-level goal (e.g., "Analyze last quarter's sales performance") into a specific question. It then constructs the JSON payload for the Genie API, including the prompt and necessary schema context.

Act: The agent sends the request to the API endpoint. It then parses the JSON response to extract the generated SQL or Python code from the content field.

Observe: The agent executes the extracted code against the Databricks environment (e.g., using the Databricks SQL Connector). The result of the execution (data or an error) becomes the observation.

Repeat: Based on the observation, the agent plans its next step. If the first query returned a list of top products, its next plan might be to generate a new query to analyze the sales trend for each of those products. This loop continues until the high-level goal is achieved.

By integrating the Genie API in this way, you can build autonomous agents that perform complex data analysis, generate reports, and monitor metrics with minimal human intervention.
