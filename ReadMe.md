# 🦘 Roo Bricks 🧱

This repository serves as a learning playground for exploring Databricks platform capabilities and features using Mockaroo as a data source.

## 🔨 Development Setup (Linux)

1. Install uv package manager
    ```shell
    curl -LsSf https://astral.sh/uv/install.sh | sh
    ```
1. Download Roo Bricks project dependencies
    ```shell
    uv sync
    ```
1. Install DuckDB for local development
    ```shell
    curl https://install.duckdb.org | sh
    ```

# 🧱 Databricks Setup

1. Generate token in Databricks UI
    1. Go to User Settings → Access Tokens
    1. Click "Generate New Token"
    1. Set expiration and copy the token
1. Authenticate the Databricks CLI
    ```shell
    databricks configure --token
    ```
1. Initialize Databricks secrets
    ```shell
    # Create new scope for secrets
    databricks secrets create-scope roo-bricks

    # Add Mockaroo API key to the new scope
    databricks secrets put-secret roo-bricks mockaroo-api-key
    ```
