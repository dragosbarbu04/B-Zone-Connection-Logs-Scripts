import json
import argparse
import sys

def extract_unique_user_ids(filename):
    """
    Reads a JSON file containing a list of objects, extracts 'userId' values,
    and returns a list of unique user IDs.

    Args:
        filename (str): The path to the input JSON file.

    Returns:
        list: A list of unique user IDs found in the file, or None if an error occurs.
    """
    print(f"Attempting to load data from: {filename}")
    try:
        # Open and load the JSON file
        with open(filename, 'r', encoding='utf-8') as f:
            data = json.load(f)

        # --- Data Validation ---
        # Ensure the loaded data is a list
        if not isinstance(data, list):
            print(f"Error: Expected JSON file to contain a list ([...]), but found type {type(data)}.", file=sys.stderr)
            # Check if it's a dict with a 'data' key as a fallback (like previous examples)
            if isinstance(data, dict) and 'data' in data and isinstance(data['data'], list):
                 print("Found list under 'data' key. Using that.")
                 data = data['data']
            else:
                 return None # Indicate failure

        # --- Extract User IDs ---
        all_user_ids = []
        print("Extracting user IDs...")
        for record in data:
            # Check if the record is a dictionary and has the 'userId' key
            if isinstance(record, dict):
                user_id = record.get('userId') # Use .get() for safety
                if user_id is not None:
                    all_user_ids.append(user_id)
                else:
                    print(f"Warning: Record found without 'userId' key: {record}", file=sys.stderr)
            else:
                print(f"Warning: Found non-dictionary item in list: {record}", file=sys.stderr)

        # --- Find Unique IDs ---
        # Convert the list to a set to remove duplicates, then back to a list
        unique_ids = list(set(all_user_ids))
        print(f"Found {len(unique_ids)} unique user IDs.")

        return unique_ids

    except FileNotFoundError:
        print(f"Error: File not found at '{filename}'", file=sys.stderr)
        return None
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON from '{filename}': {e}", file=sys.stderr)
        return None
    except Exception as e:
        print(f"An unexpected error occurred: {e}", file=sys.stderr)
        return None

if __name__ == "__main__":
    # --- Argument Parsing ---
    parser = argparse.ArgumentParser(description="Extract unique User IDs from a JSON file containing a list of connection logs.")
    parser.add_argument("filename", help="Path to the input JSON file.")
    args = parser.parse_args()

    # --- Process File ---
    unique_user_ids = extract_unique_user_ids(args.filename)

    # --- Print Results ---
    if unique_user_ids is not None:
        print("\n--- Unique User IDs ---")
        # Sort for consistent output (optional)
        unique_user_ids.sort()
        print(unique_user_ids)
        print("-----------------------")
    else:
        print("Script failed due to errors.", file=sys.stderr)
        sys.exit(1)

    print("Script finished.")
