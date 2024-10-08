# solana-cos-plugin

This is an open-source Geyser plugin for Solana.

## Description

The `solana-cos-plugin` is designed to save historical node information to disk in Cloud Object Storage (COS) format, ready to be uploaded to cloud storage.

## Features

- Prepare and save node information to disk in COS format.

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
    Make sure Solana and Cos Plugin are compiled with the same rust compiler version.

5. **Run the project:**
    ```sh
    solana-test-validator --geyser-plugin-config config.json
    ```

## Usage

To use the `solana-cos-plugin`, follow these steps:

1. **Adjust the Configuration File:**
    Edit the `config.json` file in the repository to specify the appropriate settings for your needs.

    Example `config.json`:
    ```json
    {
        "libpath": "./target/release/libsolana_cos_plugin.so",
        "workspace": "./workspace",
        "slot_range": 1000,
        "commit_slot_delay": 500
    }
    ```

    - **`libpath`**: Path to the plugin's shared library.
    - **`workspace`**: A working folder where the plugin will store files while running.
    - **`slot_range`**: How many slots per folder to store on file storage.
    - **`commit_slot_delay`**: How many slots to wait before moving a slot range from staging to storage.

2. **Start the Solana Validator with the Geyser Plugin:**
    Run the following command in your project directory:
    ```sh
    solana-test-validator --geyser-plugin-config config.json
    ```

    Ensure you have completed the installation steps before running the Solana validator with the Geyser plugin.

## Architecture

The architecture of the `solana-cos-plugin` is centered around several key classes:

- **`GeyserPluginCosConfig`**: This is the configuration class of the plugin. It reads and validates the configuration parameters from the `config.json` file.
- **`GeyserPluginCos`**: This is the main plugin interface that implements the `GeyserPlugin` interface from Solana. It handles the interaction with Solana's data stream and coordinates data store to disk.
- **`StorageManager`**: This class manages finalized slots storage. It prepares and save each slot on disk, so that they can later be uploaded to COS.

### Live syncing to COS

The `solana-cos-plugin`, despite its name, doesn't actually upload the data to COS directly.
Rather, it uses a separate process called the [Solana Syncer](https://github.com/bwarelabs/solana-syncer).

The flow of the data is as follows:

```mermaid
graph TD
    A -->|Dumps data to storage| B[Disk]
    C[Solana Syncer] -->|Reads data from storage| B
    C -->|Uploads data to COS| D[COS]

    subgraph Solana Validator Node
        A[Solana COS Plugin]
    end
```

The plugin serializes data in the same format as BigTable (i.e., using protobuf and compression) and stores it
in a staging location on local storage. Once the data is fully written to disk, it is moved from the staging location
to the final location. From there, the syncer will pick it up, upload it to COS, and then delete the local copy.

## Contributing

We welcome contributions! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for details on how to contribute.

## License

This project is licensed under the Apache 2.0 License. See the [LICENSE](LICENSE) file for more information.
