# 🦘 Mockaroo Playpen 🥅

This repository serves as a learning playground for exploring Databricks platform capabilities and features.

## 🔨 Setup (Linux)

1. Generate token in Databricks UI
    1. Go to User Settings → Access Tokens
    1. Click "Generate New Token"
    1. Set expiration and copy the token
1. Setup Python virtual environment
    ```shell
    python3.11 -m venv .env
    ```
1. Install and authenticate the Databricks CLI
    ```shell
    pip install databricks-cli

    databricks configure --token
    ```
1. Install the Databricks SDK
    ```shell
    pip install 'databricks-sdk[notebook]'
    ```

### 🔌 Databricks VSCode Extension

1. Install `databricks-connect` Python package
    ```shell
    pip install databricks-connect
    ```
