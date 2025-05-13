# Cryptocurrency Price Data Pipeline

An Apache Airflow data pipeline that fetches currency exchange rates from the National Bank of Poland (NBP) API, processes the data, and stores it in a MySQL database.

## Features

- Daily automated fetching of currency exchange rates
- Data transformation and cleaning pipeline
- UTC timestamp tracking for each data point
- MySQL database storage
- Error handling and retry mechanisms
- Email notifications on failure

## Architecture

The pipeline consists of several tasks:

1. `fetch_currencies`: Retrieves current exchange rates from NBP API
2. `transform_currency_data`: Processes and formats the raw data
3. `add_time_and_metadata`: Adds timestamps and source information
4. `validate_and_clean_data`: Removes duplicates and invalid entries
5. `save_to_mysql`: Stores processed data in MySQL database

## Prerequisites

- Docker and Docker Compose
- Python 3.x
- MySQL Server
- WSL (if running on Windows)

## Environment Variables

Create a `.env` file based on `env.example` with the following variables:

```
AIRFLOW_UID=50000
COINGECKO_API_URL=https://api.coingecko.com/api/v3/simple/price
POSTGRES_CONN=postgresql://<USER>:<PASS>@postgres:5432/airflow
SMTP_HOST=<YOUR_SMTP_HOST>
SMTP_PORT=<YOUR_SMTP_PORT>
SMTP_USER=<YOUR_SMTP_USER>
SMTP_PASSWORD=<YOUR_SMTP_PASSWORD>
SMTP_MAIL_FROM=<YOUR_EMAIL>
ADMIN_PASSWORD=<YOUR PASSWORD>
FERNET_KEY=<YOUR FERNET KEY>
LOCAL_USER_ID=1000
LOCAL_GROUP_ID=1000
wsl_ip=<YOUR_WSL_IP_OR_SYSTEM_IP>
```

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd crypto-airflow
```

2. Create and configure the `.env` file

3. Start the containers:
```bash
docker-compose up -d
```

## Development

To develop in a containerized environment, this project includes a VS Code devcontainer configuration. Required extensions:
- Python
- Pylance

## Project Structure

```
├── .devcontainer/         # Development container configuration
├── config/               # Airflow configuration
├── dags/                # Airflow DAG definitions
├── logs/                # Airflow logs
├── plugins/             # Airflow plugins
├── scripts/             # Utility scripts
├── .env.example         # Example environment variables
├── docker-compose.yml   # Docker services configuration
└── README.md           # This file
```

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request
