import json
import sys
import time
import argparse
import os
from dotenv import load_dotenv
from seleniumwire import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, ElementNotInteractableException
import concurrent.futures
import threading # For Lock
import math # For ceiling division

# --- Credentials & Configuration ---
load_dotenv()
LOGIN_URL = "https://v.b-zone.ro/account/login"
USERNAME_ENV = os.getenv("USERNAME")
PASSWORD_ENV = os.getenv("PASSWORD")

# Element Locators
USERNAME_FIELD_LOCATOR = (By.XPATH, "//*[@id='page-container']/div/div/div[1]/div/div/div[2]/div/form/div[1]/div[1]/input")
PASSWORD_FIELD_LOCATOR = (By.XPATH, "//*[@id='page-container']/div/div/div[1]/div/div/div[2]/div/form/div[1]/div[2]/input")
LOGIN_BUTTON_LOCATOR = (By.XPATH, "//*[@id='page-container']/div/div/div[1]/div/div/div[2]/div/form/div[2]/div/button")
POST_LOGIN_ELEMENT_LOCATOR = (By.XPATH, "//*[@id='page-header-search-input2']")
DATA_PAGE_URL = "https://v.b-zone.ro/admin/connection-logs"
LENGTH_DROPDOWN_LOCATOR = (By.XPATH, "//*[@id='table_length']/label/select")
LENGTH_OPTION_100_LOCATOR = (By.XPATH, "//*[@id='table_length']/label/select/option[4]")
USER_ID_SEARCH_LOCATOR = (By.XPATH, "//*[@id='example-group1-input2']")
NEXT_PAGE_BUTTON_LOCATOR = (By.XPATH, "//*[@id='table_next']/a")
NEXT_PAGE_DISABLED_LOCATOR = (By.XPATH, "//*[@id='table_next' and contains(@class, 'disabled')]")

DATA_REQUEST_PATH = "/admin/connectionLogs"
JSON_DATA_KEY = 'data'
DEFAULT_MAX_LOGS_PER_USER = 200
DEFAULT_OUTPUT_FILENAME = "all_connection_logs.json"
DEFAULT_ID_FILENAME = "output.txt"
POST_LOGIN_TIMEOUT = 45 # Increased timeout for initial login, can be slow
ACTION_WAIT_TIME = 3
DEFAULT_MAX_WORKERS = 4

# --- WebDriver Options ---
def get_chrome_options():
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--window-size=1920,1080')
    options.add_argument('--blink-settings=imagesEnabled=false')
    options.add_experimental_option('excludeSwitches', ['enable-logging'])
    return options

# --- Core Functions ---
def login(driver, wait, username_to_use, password_to_use, post_login_timeout_val):
    """Logs into the website. Assumes driver and wait are provided."""
    instance_id = driver.session_id[:8] if driver and hasattr(driver, 'session_id') else 'N/A'
    print(f"Instance {instance_id}: Navigating to login page: {LOGIN_URL}")
    driver.get(LOGIN_URL)
    print(f"Instance {instance_id}: Attempting login...")
    try:
        user_field = wait.until(EC.presence_of_element_located(USERNAME_FIELD_LOCATOR))
        pass_field = wait.until(EC.presence_of_element_located(PASSWORD_FIELD_LOCATOR))
        login_button_element = wait.until(EC.presence_of_element_located(LOGIN_BUTTON_LOCATOR))

        user_field.send_keys(username_to_use)
        pass_field.send_keys(password_to_use)
        print(f"Instance {instance_id}: Credentials entered, attempting JavaScript click on login button...")

        driver.execute_script("arguments[0].scrollIntoView(true);", login_button_element)
        time.sleep(0.5)
        driver.execute_script("arguments[0].click();", login_button_element)
        print(f"Instance {instance_id}: JavaScript click executed on login button.")

        login_wait = WebDriverWait(driver, post_login_timeout_val)
        login_wait.until(EC.presence_of_element_located(POST_LOGIN_ELEMENT_LOCATOR))
        print(f"Instance {instance_id}: Login successful.")
        return True
    except TimeoutException:
        print(f"Instance {instance_id}: ERROR: Timed out waiting for login elements or confirmation.", file=sys.stderr)
        # driver.save_screenshot(f"login_timeout_error_instance_{instance_id}.png")
        return False
    except Exception as e:
        print(f"Instance {instance_id}: ERROR during login: {e}", file=sys.stderr)
        # driver.save_screenshot(f"login_generic_error_instance_{instance_id}.png")
        return False

def initialize_and_login_driver(username_val, password_val, post_login_timeout_val):
    """Initializes a single WebDriver instance and logs it in."""
    driver = None
    try:
        driver = webdriver.Chrome(options=get_chrome_options())
        wait = WebDriverWait(driver, 20) # General wait for this driver instance
        if login(driver, wait, username_val, password_val, post_login_timeout_val):
            return driver, wait
        else:
            if driver:
                driver.quit()
            return None, None
    except Exception as e:
        print(f"ERROR: Failed to initialize or login WebDriver instance: {e}", file=sys.stderr)
        if driver:
            driver.quit()
        return None, None

def capture_latest_data_request(driver):
    target_request = None
    for request in reversed(driver.requests):
        if request.method == 'POST' and DATA_REQUEST_PATH in request.url:
            target_request = request
            break
    if not target_request or not target_request.response: return None
    if target_request.response.status_code == 200 and \
       target_request.response.headers.get('Content-Type', '').startswith('application/json'):
        response_body = target_request.response.body.decode('utf-8', errors='ignore')
        try:
            raw_data = json.loads(response_body)
            return raw_data.get(JSON_DATA_KEY) if isinstance(raw_data, dict) and JSON_DATA_KEY in raw_data and isinstance(raw_data[JSON_DATA_KEY], list) else (raw_data if isinstance(raw_data, list) else None)
        except json.JSONDecodeError: return None
    return None

def search_and_fetch_logs_for_id(driver, wait, user_id_to_search, max_logs, action_wait):
    """Fetches logs for a single user ID. Assumes driver is ALREADY LOGGED IN."""
    user_logs = []
    instance_id = driver.session_id[:8]
    print(f"Instance {instance_id}, User {user_id_to_search}: Starting fetch.")
    try:
        # Navigate to data page for each new User ID search to ensure clean state
        del driver.requests
        driver.get(DATA_PAGE_URL)
        time.sleep(action_wait / 2) # Shorter wait if page elements load fast from cache

        try:
            length_dropdown_element = wait.until(EC.element_to_be_clickable(LENGTH_DROPDOWN_LOCATOR))
            length_dropdown_element.click()
            time.sleep(0.5)
            option_100 = wait.until(EC.element_to_be_clickable(LENGTH_OPTION_100_LOCATOR))
            option_100.click()
            time.sleep(action_wait / 2) # Wait for table to reload
            del driver.requests
        except Exception as e:
            print(f"Instance {instance_id}, User {user_id_to_search}: WARN - Could not set page length: {e}", file=sys.stderr)

        search_bar = wait.until(EC.visibility_of_element_located(USER_ID_SEARCH_LOCATOR))
        search_bar.clear()
        search_bar.send_keys(str(user_id_to_search))
        del driver.requests
        search_bar.send_keys(Keys.RETURN)
        time.sleep(action_wait)

        page_count = 1
        while len(user_logs) < max_logs:
            page_data = capture_latest_data_request(driver)
            if page_data is None or not page_data: break
            user_logs.extend(page_data)
            if len(user_logs) >= max_logs: break

            try:
                driver.find_element(*NEXT_PAGE_DISABLED_LOCATOR)
                break
            except NoSuchElementException:
                try:
                    next_button = wait.until(EC.element_to_be_clickable(NEXT_PAGE_BUTTON_LOCATOR))
                    driver.execute_script("arguments[0].scrollIntoView(false);", next_button)
                    time.sleep(0.5)
                    del driver.requests
                    driver.execute_script("arguments[0].click();", next_button)
                    time.sleep(action_wait)
                    page_count += 1
                except Exception: break # Break on any pagination error for this ID
        
        print(f"Instance {instance_id}, User {user_id_to_search}: Collected {len(user_logs[:max_logs])} logs.")
        return user_id_to_search, user_logs[:max_logs]
    except Exception as e:
        print(f"Instance {instance_id}, User {user_id_to_search}: ERROR - {e}", file=sys.stderr)
        # driver.save_screenshot(f"fetch_error_user_{user_id_to_search}_instance_{instance_id}.png")
        return user_id_to_search, user_logs # Return partial if any

# --- Worker Function for ThreadPool ---
def process_id_batch_worker(driver_instance, wait_instance, id_batch, max_logs_per_user_val, action_wait_val):
    """Worker function to process a batch of User IDs using a pre-logged-in WebDriver instance."""
    instance_id = driver_instance.session_id[:8]
    print(f"Instance {instance_id}: Starting worker for batch of {len(id_batch)} IDs.")
    results_for_batch = []
    for user_id in id_batch:
        print(f"Instance {instance_id}: Processing User ID: {user_id}")
        _, logs = search_and_fetch_logs_for_id(driver_instance, wait_instance, user_id, max_logs_per_user_val, action_wait_val)
        if logs is not None: # Even an empty list is a valid result (means no logs found)
            results_for_batch.append({'user_id': user_id, 'logs': logs})
        # If session becomes invalid or major error, this worker might stop processing further IDs.
        # More robust error handling could re-queue remaining IDs in its batch.
        # For now, it continues with the next ID in its batch.
    print(f"Instance {instance_id}: Worker finished batch. Processed {len(results_for_batch)} IDs successfully.")
    return results_for_batch


# --- File and Data Handling ---
def read_ids_from_file(filename):
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            content = f.read()
            ids = [item.strip() for item in content.split() if item.strip().isdigit()]
            print(f"Read {len(ids)} IDs from {filename}")
            return ids
    except FileNotFoundError:
        print(f"ERROR: ID file '{filename}' not found.", file=sys.stderr)
        return []
    except Exception as e:
        print(f"ERROR: Could not read IDs from '{filename}': {e}", file=sys.stderr)
        return []

def load_processed_user_ids(filename):
    """Loads existing logs and returns a set of User IDs for whom logs exist."""
    processed_ids = set()
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            data = json.load(f) # Expects a list of log entries
        # Assuming each log entry might not directly have UserID,
        # this needs to be smarter or log data should include UserID.
        # If output is list of {"user_id": id, "logs": [...]}, then:
        # for item in data:
        #     if isinstance(item, dict) and 'user_id' in item:
        #         processed_ids.add(str(item['user_id']))
        
        # For now, this function won't be perfect for resuming unless log structure is known.
        # A simpler resume: If the script saves logs associated with a user_id clearly.
        # Let's assume the output file is a flat list of all log entries.
        # We can't reliably get *distinct user IDs* from a flat list of logs unless each log item has a UserID field.
        # For this script, if the output is a list of lists, where each inner list is logs for one user, it's also hard.
        # The NEW structure will be a list of dictionaries: [{'user_id':id, 'logs':[...]}]
        
        for entry in data:
            if isinstance(entry, dict) and 'user_id' in entry:
                processed_ids.add(str(entry['user_id'])) # Ensure user_id is string for comparison
        if processed_ids:
            print(f"Loaded {len(processed_ids)} already processed User IDs from {filename}.")
        return processed_ids
    except FileNotFoundError:
        return set()
    except json.JSONDecodeError:
        print(f"ERROR: Could not decode JSON from '{filename}'. File might be corrupted.", file=sys.stderr)
        return set()
    except Exception: # Catch other potential errors during loading
        return set()


def save_results_to_json(all_results, filename, lock):
    """Saves the collected results to a JSON file."""
    # all_results is expected to be a list of dicts: [{'user_id': id, 'logs': [...]}, ...]
    with lock:
        # print(f"Attempting to save results for {len(all_results)} User IDs to {filename}...")
        try:
            # To avoid duplicates if resuming, we might want to merge based on user_id
            # For now, just dump what we have. If resuming, filter IDs before processing.
            current_data_to_save = []
            existing_ids = set()
            
            # Load existing data to merge correctly
            try:
                with open(filename, 'r', encoding='utf-8') as f_read:
                    current_data_to_save = json.load(f_read)
                    for item in current_data_to_save:
                        if isinstance(item, dict) and 'user_id' in item:
                            existing_ids.add(str(item['user_id']))
            except (FileNotFoundError, json.JSONDecodeError):
                current_data_to_save = [] # Start fresh if file not found or corrupt
                existing_ids = set()

            # Add new results, replacing if user_id already exists (or just append if duplicates are okay/handled by pre-filtering)
            # For simplicity with pre-filtering, we assume `all_results` only contains new data.
            # If we don't pre-filter IDs, we need to merge carefully.
            # Since we *do* pre-filter IDs, `all_results` contains data for NEWLY processed IDs.
            # So, we just append them to what was already in the file.
            
            # Let's refine: The `all_results` parameter should be the *complete* list to save.
            # The main function will manage this complete list.
            
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(all_results, f, indent=2, ensure_ascii=False)
            print(f"Save successful to {filename}. Total User ID entries: {len(all_results)}")
        except Exception as e:
            print(f"ERROR: Failed to save data to JSON file '{filename}': {e}", file=sys.stderr)


# --- Main Execution Logic ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch connection logs concurrently using pre-logged-in browser instances.")
    parser.add_argument("--id-file", type=str, default=DEFAULT_ID_FILENAME,
                        help=f"File containing space-separated User IDs to process (default: {DEFAULT_ID_FILENAME})")
    parser.add_argument("--max-logs-per-user", type=int, default=DEFAULT_MAX_LOGS_PER_USER,
                        help=f"Maximum number of logs to fetch per user ID (default: {DEFAULT_MAX_LOGS_PER_USER})")
    parser.add_argument("-o", "--output", default=DEFAULT_OUTPUT_FILENAME,
                        help=f"Output JSON filename (default: {DEFAULT_OUTPUT_FILENAME})")
    parser.add_argument("--max-workers", type=int, default=DEFAULT_MAX_WORKERS,
                        help=f"Number of concurrent browser instances (default: {DEFAULT_MAX_WORKERS})")
    parser.add_argument("--username", type=str, default=USERNAME_ENV, help="Login username (overrides .env)")
    parser.add_argument("--password", type=str, default=PASSWORD_ENV, help="Login password (overrides .env)")

    args = parser.parse_args()

    current_username = args.username
    current_password = args.password

    if not current_username or not current_password or "YOUR_USERNAME" in current_username or "YOUR_PASSWORD" in current_password:
        print("ERROR: USERNAME and PASSWORD must be set via .env file or command-line arguments.", file=sys.stderr)
        sys.exit(1)

    all_user_ids_from_file = read_ids_from_file(args.id_file)
    if not all_user_ids_from_file:
        print("No User IDs to process from file. Exiting.")
        sys.exit(0)

    # Load already processed User IDs to avoid re-processing
    processed_user_ids_set = load_processed_user_ids(args.output)
    
    ids_to_process_queue = [
        uid for uid in all_user_ids_from_file if uid not in processed_user_ids_set
    ]

    if not ids_to_process_queue:
        print("All User IDs from the file have already been processed. Exiting.")
        sys.exit(0)
    
    print(f"Found {len(processed_user_ids_set)} already processed IDs. Need to process {len(ids_to_process_queue)} new IDs.")

    active_drivers_waits = []
    print(f"Attempting to initialize and log in {args.max_workers} browser instances...")
    for i in range(args.max_workers):
        print(f"Initializing instance {i+1}...")
        driver, wait = initialize_and_login_driver(current_username, current_password, POST_LOGIN_TIMEOUT)
        if driver and wait:
            active_drivers_waits.append({'driver': driver, 'wait': wait, 'id': driver.session_id[:8]})
            print(f"Instance {driver.session_id[:8]} successfully initialized and logged in.")
        else:
            print(f"Failed to initialize or login instance {i+1}.")
    
    if not active_drivers_waits:
        print("ERROR: No WebDriver instances could be successfully initialized and logged in. Exiting.", file=sys.stderr)
        sys.exit(1)

    print(f"Successfully initialized {len(active_drivers_waits)} browser instances.")

    # Distribute IDs among active drivers
    num_active_drivers = len(active_drivers_waits)
    ids_per_driver = math.ceil(len(ids_to_process_queue) / num_active_drivers)
    
    id_batches = []
    for i in range(num_active_drivers):
        start_index = i * ids_per_driver
        end_index = start_index + ids_per_driver
        batch = ids_to_process_queue[start_index:end_index]
        if batch: # Only add if batch is not empty
            id_batches.append(batch)

    # Load existing data to append new results
    # The `save_results_to_json` handles merging in a basic way, but it's better if main list is complete.
    # Let's load the full existing dataset. Results from threads will be for new IDs.
    final_results_list = []
    try:
        with open(args.output, 'r', encoding='utf-8') as f_initial:
            final_results_list = json.load(f_initial)
            print(f"Loaded {len(final_results_list)} existing result entries from {args.output}")
    except (FileNotFoundError, json.JSONDecodeError):
        print(f"Starting with an empty result list (or {args.output} is new/corrupt).")
        final_results_list = []


    file_save_lock = threading.Lock()
    processed_count = 0
    total_to_process_this_run = len(ids_to_process_queue)

    with concurrent.futures.ThreadPoolExecutor(max_workers=num_active_drivers) as executor:
        future_to_driver_id = {}
        for i in range(len(id_batches)): # Should be min(num_active_drivers, len(id_batches))
            if i < num_active_drivers: # Ensure we don't try to use more drivers than available
                driver_info = active_drivers_waits[i]
                batch_to_process = id_batches[i]
                print(f"Submitting batch of {len(batch_to_process)} IDs to instance {driver_info['id']}")
                future = executor.submit(process_id_batch_worker, 
                                         driver_info['driver'], driver_info['wait'], 
                                         batch_to_process, args.max_logs_per_user, ACTION_WAIT_TIME)
                future_to_driver_id[future] = driver_info['id']

        for future in concurrent.futures.as_completed(future_to_driver_id):
            driver_instance_id = future_to_driver_id[future]
            try:
                batch_results = future.result() # This is a list of {'user_id': id, 'logs': logs}
                if batch_results:
                    final_results_list.extend(batch_results)
                    processed_count += len(batch_results) # Count how many user_ids were in the results
                    print(f"Instance {driver_instance_id}: Batch processed. Results for {len(batch_results)} User IDs collected.")
                else:
                    print(f"Instance {driver_instance_id}: Batch processed but returned no results.")
                
                print(f"Overall Progress: Approximately {processed_count}/{total_to_process_this_run} new IDs handled in this run.")
                # Save incrementally after each batch is processed
                save_results_to_json(final_results_list, args.output, file_save_lock)

            except Exception as exc:
                print(f"Instance {driver_instance_id} generated an exception for its batch: {exc}", file=sys.stderr)
            
    print("\n--- All task batches completed ---")
    # Final save
    save_results_to_json(final_results_list, args.output, file_save_lock)

    # Cleanup: Close all WebDriver instances
    print("Closing all WebDriver instances...")
    for driver_info in active_drivers_waits:
        try:
            if driver_info['driver']:
                driver_info['driver'].quit()
                print(f"Instance {driver_info['id']} closed.")
        except Exception as e:
            print(f"Error closing instance {driver_info.get('id', 'N/A')}: {e}", file=sys.stderr)

    print(f"Script finished. Total User ID entries in output file: {len(final_results_list)}")
