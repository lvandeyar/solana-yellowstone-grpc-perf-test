# GRPC Endpoint Performance Tracker

This project is a Go application designed to connect to multiple GRPC endpoints, track their performance, and report metrics such as transactions per second (TPS), latency, and error rates. It is particularly useful for monitoring the performance of blockchain nodes or other GRPC services.

## Features

- Connects to multiple GRPC endpoints.
- Tracks and reports metrics like TPS, latency, and error rates.
- Supports secure connections using TLS.
- Configurable via environment variables.

## Prerequisites

- Go 1.16 or later
- Access to GRPC endpoints

## Setup

1. **Clone the repository**:
   ```bash
   git clone <repository-url>
   cd <repository-directory>
   ```

2. **Create a `.env` file**:
   Create a `.env` file in the root directory of the project and add your provider details:
   ```plaintext
   PROVIDER1_HOST=
   PROVIDER1_PORT=
   PROVIDER1_TOKEN=
   PROVIDER1_USE_TLS=false

   QUICKNODE_HOST=
   QUICKNODE_PORT=
   QUICKNODE_TOKEN=
   QUICKNODE_USE_TLS=true
   ```

3. **Install dependencies**:
   Ensure you have the necessary Go packages installed. You can use `go mod tidy` to install any missing dependencies.

4. **Run the application**:
   ```bash
   go run main.go
   ```

## Usage

- The application will automatically connect to the endpoints specified in the `.env` file.
- It will run for a predefined duration and then output a performance report comparing the endpoints.

## Configuration

- Modify the `.env` file to change the endpoints and their connection details.
- Adjust the `TestDurationSec` constant in `main.go` to change the test duration.

## Security Note

- **Provider1** is configured to use an insecure connection (`UseTLS=false`) for testing purposes. This is intentional to demonstrate how the application handles both secure and insecure connections. Ensure that in a production environment, all connections are secured using TLS.
