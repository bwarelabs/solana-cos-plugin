# solana-cos-plugin

This is an open-source Geyser plugin for Solana.

## Description

The `solana-cos-plugin` is designed to upload historical node information to Cloud Object Storage (COS) in Tencent and HBase. Solana currently stores this information in BigTable, and this plugin extends support to other storage solutions.

## Features

- Uploads archive data to Tencent COS
- Uploads archive data to HBase

## Installation

To install the `solana-cos-plugin`, follow these steps:

1. **Clone the repository:**
    ```sh
    git clone https://github.com/bwarelabs/solana-cos-plugin.git
    ```
2. **Change to the project directory:**
    ```sh
    cd solana-cos-plugin
    ```
3. **Build the project with Cargo:**
    ```sh
    cargo build --release
    ```
4. **Prepare the Solana validator:**
    Ensure you have Solana installed and accessible in your system's PATH. Refer to the [Solana installation guide](https://docs.solana.com/cli/install-solana-cli-tools) if needed.

5. **Run the project:**
    ```sh
    solana-validator --geyser-plugin-config config.json
    ```

## Usage

To use the `solana-cos-plugin`, follow these steps:

1. **Adjust the Configuration File:**
    Edit the `config.json` file in the repository to specify the settings for Tencent COS and HBase. This file contains the necessary configuration parameters such as credentials, endpoints, and other relevant settings.

    Example `config.json`:
    ```json
    {
        "libpath": "./target/debug/libblast_cos_plugin.so",
        "workspace": "./workspace",
        "max_file_size_mb": 100,
        "slot_range": 1000
    }
    ```

    - **`libpath`**: Path to the plugin's shared library.
    - **`workspace`**: A working folder where the plugin will store files while running.
    - **`max_file_size_mb`**: Maximum file size for storing data. When this size is reached, a new file is created.
    - **`slot_range`**: How many slots per folder to store on file storage.

2. **Start the Solana Validator with the Geyser Plugin:**
    Run the following command in your project directory:
    ```sh
    solana-validator --geyser-plugin-config config.json
    ```

    Ensure you have completed the installation steps before running the Solana validator with the Geyser plugin.

## Architecture

The architecture of the `solana-cos-plugin` is centered around several key classes:

- **`GeyserPluginCosConfig`**: This is the configuration class of the plugin. It reads and validates the configuration parameters from the `config.json` file.
- **`GeyserPluginCos`**: This is the main plugin interface that implements the `GeyserPlugin` interface from Solana. It handles the interaction with Solana's data stream and coordinates data upload to COS and HBase.
- **`LogManager`**: This class manages the persistent storage. It saves all data received via the `GeyserPlugin` interface and is used to recover from shutdowns, failover exceptions, and other interruptions.

> Note: Additional classes will be added later.

### Architecture Diagram

> TODO: ![Architecture Diagram](path/to/architecture_diagram.png)

## Contributing

We welcome contributions! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for details on how to contribute.

## License

This project is licensed under the Apache 2.0 License. See the [LICENSE](LICENSE) file for more information.

> Note: This project is currently WIP! Do not even try to compile it!