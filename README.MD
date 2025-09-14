# B-Zone Connection & Multi-Account Analyzer

This is a toolkit of Python scripts designed for B-Zone server administrators to efficiently download, analyze, and act on user connection logs. The primary purpose is to detect players who are sharing accounts by cross-referencing IPs, Hardware IDs (HWIDs), and Social Club IDs.

## Table of Contents
- [Features](#features)
- [Workflow](#workflow)
- [File Descriptions](#file-descriptions)
- [Setup & Installation](#setup--installation)
- [Usage Guide](#usage-guide)
  - [Step 1: Prepare User IDs](#step-1-prepare-user-ids)
  - [Step 2: Fetch Connection Logs](#step-2-fetch-connection-logs)
  - [Step 3: Analyze for Shared Accounts](#step-3-analyze-for-shared-accounts)
- [Contributing](#contributing)


## Features

- **Concurrent Log Fetching**: Utilizes multiple browser instances to download logs for many users simultaneously, drastically reducing data collection time.
- **Intelligent Resumption**: The script saves its progress and can resume fetching logs without re-downloading data for already processed users.
- **In-Depth Analysis**: Identifies shared accounts by checking for multiple users on the same IP, HWID, or Social Club ID.
- **Primary Account Detection**: Attempts to identify the "likely primary" user of a shared identifier based on connection frequency and recency.
- **Automated Workflow**: A series of scripts that streamline the process from raw data to actionable analysis.
- **Credential Management**: Securely handles your login credentials using a `.env` file.

## Workflow

The toolkit follows a clear, multi-step process:

1.  **Prepare a List of User IDs**: Start by generating a clean, formatted list of user IDs that you want to investigate.
2.  **Fetch Logs from Server**: Use the list of IDs to automatically download all relevant connection logs from the admin panel.
3.  **Analyze Log Data**: Process the downloaded JSON file to identify which users are connected by shared identifiers.
4.  **Review and Act**: Review the generated analysis report and use the provided helper script to apply sanctions if necessary.

## File Descriptions

| File                       | Description                                                                                                                              |
| -------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------- |
| `connectionLogsGrabber.py` | The core script. Logs into the admin panel and concurrently downloads connection logs for all user IDs provided in an input file.          |
| `multiAccountChecker.py`   | Analyzes the log file from the grabber. It finds and reports on users who share IPs, HWIDs, or Social Club IDs.                             |
| `getUniqueIds.py`          | A helper utility to extract a list of unique `userId` values from a larger, complex JSON file.                                           |
| `uniqueIds.py`             | A helper utility that takes a simple text file with one ID per line and formats it into a single, space-separated line for the grabber. |
| `sanctions.py`             | An automation script that uses `pyautogui` to type in-game warning commands for a predefined list of users. **Use with caution.** |

## Setup & Installation

Follow these steps to get the toolkit running on your local machine.


1.  **Install Dependencies:**
    It's recommended to use a virtual environment. The required packages are `selenium-wire` for browser automation, `python-dotenv` for managing credentials, and `pyautogui` for the sanctions script.
    ```bash
    pip install selenium-wire python-dotenv pyautogui
    ```

2.  **Set Up Credentials:**
    Create a file named `.env` in the root directory of the project. This file will securely store your login credentials. Add the following lines, replacing the placeholder values with your actual admin panel username and password:
    ```env
    USERNAME="YOUR_ADMIN_USERNAME"
    PASSWORD="YOUR_ADMIN_PASSWORD"
    ```

## Usage Guide

### Step 1: Prepare User IDs

You need a file containing the user IDs you want to check, formatted as a single line with spaces between each ID.

-   **Option A (If you have a simple list):** Create a file named `message.txt` and paste one user ID per line. Then run `uniqueIds.py` to format it correctly.
    ```bash
    python uniqueIds.py
    ```
    This will generate an `output.txt` file, which is ready for the next step.

-   **Option B (If you have a complex JSON):** If you have a larger JSON file (e.g., from another tool) and need to extract user IDs from it, use `getUniqueIds.py`.
    ```bash
    python getUniqueIds.py your_data.json > output.txt
    ```

### Step 2: Fetch Connection Logs

Now, use the formatted ID file (`output.txt`) to fetch the logs from the server.

-   Run the `connectionLogsGrabber.py` script. You can adjust the number of concurrent workers (browsers) with the `--max-workers` flag for faster performance.
    ```bash
    # Basic usage
    python connectionLogsGrabber.py --id-file output.txt

    # With more workers for faster downloads
    python connectionLogsGrabber.py --id-file output.txt --max-workers 8 --output all_logs.json
    ```
-   The script will save all collected data into `all_connection_logs.json` (or the filename you specify with `--output`). If the script is interrupted, you can simply run it again, and it will resume where it left off.

### Step 3: Analyze for Shared Accounts

With the log data collected, you can now run the analysis.

-   Point the `multiAccountChecker.py` script to your JSON log file. The `--analyze` flag is required to perform the analysis.
    ```bash
    python multiAccountChecker.py all_logs.json --analyze --output-analysis results.txt
    ```
-   This command will:
    1.  Read the `all_logs.json` file.
    2.  Perform the analysis to find shared identifiers.
    3.  Save a detailed, human-readable report to `results.txt`.

Review the `results.txt` file to see which accounts are flagged for sharing.

### Step 4: Apply Sanctions (Optional)

**Warning:** This script automates keyboard and mouse input. It will take control of your computer to type commands. Use it responsibly and only when you are sure about the sanctions.

1.  Open the `sanctions.py` file and edit the `shared` list to include the user IDs you wish to warn.
2.  Run the script from your terminal.
    ```bash
    python sanctions.py
    ```
3.  You will have a 5-second delay. **Quickly switch to and focus the target game or application window.** The script will then begin typing the warn commands automatically.
