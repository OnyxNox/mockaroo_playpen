# 🦘 Roo Data 📊

Roo Data is a tool used to build fake datasets using the Mockaroo API.

## 🔨 Setup (Linux)

1. Install uv package manager
    ```shell
    curl -LsSf https://astral.sh/uv/install.sh | sh
    ```
1. Download project dependencies
    ```shell
    uv sync
    ```
1. Obtain a Mockaroo API key from the [Mockaroo API Docs](https://mockaroo.com/docs) (sign in is required to get an API key)
1. Copy `.env.example` to `.env` and configure your `MOCKAROO_API_KEY`

## ⬇️ Download Fake Data

1. **(EXAMPLE)** Generate and download fake data for the Lorem Ipsum Inc. Users schema
    ```shell
    uv run src/mockaroo_connector.py .data/lorem_ipsum_inc/users.schema.json
    ```
1. **(EXAMPLE)** Generate and download fake data for all Lorem Ipsum Inc. schemas
    ```shell
    uv run src/mockaroo_connector.py .data/lorem_ipsum_inc/
    ```