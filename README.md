# Go Redis Server

A simple Redis server implementation in Go.

## Features

*   Supports basic Redis commands.
*   Handles network communication with clients.
*   In-memory data storage.

## Getting Started

### Prerequisites

*   Go 1.x

### Installation

1.  Clone the repository:
    ```sh
    git clone https://github.com/your-username/go-redis-server.git
    ```
2.  Go to the project directory:
    ```sh
    cd go-redis-server
    ```
3.  Build the executable:
    ```sh
    go build
    ```

### Usage

Run the server:

```sh
./go-redis-server
```

## Project Structure

*   `main.go`: Main application entry point.
*   `command/`: Handles Redis commands.
*   `network/`: Manages network connections.
*   `resp/`: Implements the RESP (REdis Serialization Protocol).
*   `storage/`: Provides in-memory data storage.
