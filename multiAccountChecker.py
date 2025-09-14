import json
import argparse
import sys
from collections import defaultdict
# Counter is no longer directly used for the map's value type
# from collections import Counter 

def load_data_from_file(filename):
    """
    Loads log records from a JSON file.
    Handles both the new structure (list of {'user_id':id, 'logs':[]}) 
    and the old flat list structure.
    """
    print(f"Attempting to load data from: {filename}")
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            raw_data = json.load(f)
    except FileNotFoundError:
        print(f"Error: File not found at '{filename}'", file=sys.stderr)
        return None
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON from '{filename}': {e}", file=sys.stderr)
        return None
    except Exception as e:
        print(f"An unexpected error occurred while loading data: {e}", file=sys.stderr)
        return None

    flat_log_records = []

    if isinstance(raw_data, list):
        if not raw_data:
            print("JSON file loaded an empty list.")
            return [] 

        first_element = raw_data[0]
        if isinstance(first_element, dict) and 'user_id' in first_element and 'logs' in first_element and isinstance(first_element['logs'], list):
            print("Detected new JSON structure (list of user-grouped logs). Flattening...")
            for user_group_record in raw_data:
                if isinstance(user_group_record, dict) and 'logs' in user_group_record and isinstance(user_group_record['logs'], list):
                    flat_log_records.extend(user_group_record['logs'])
                else:
                    print(f"Warning: Skipping an item in the user-grouped list as it doesn't match expected structure: {type(user_group_record)}", file=sys.stderr)
            print(f"Successfully loaded and flattened {len(flat_log_records)} individual log records.")
            return flat_log_records
        else:
            print("Assuming old JSON structure (flat list of log records).")
            if all(isinstance(item, dict) for item in raw_data):
                print(f"Successfully loaded {len(raw_data)} records (old structure).")
                return raw_data 
            else:
                print("Error: Loaded a list, but its elements are not all dictionaries (for old structure) and it doesn't match new structure.", file=sys.stderr)
                return None

    elif isinstance(raw_data, dict) and 'data' in raw_data and isinstance(raw_data['data'], list):
        print("Found list under 'data' key. Using that as a flat list.")
        flat_log_records = raw_data['data']
        print(f"Successfully loaded {len(flat_log_records)} records from 'data' key.")
        return flat_log_records
    else:
        print(f"Error: Expected JSON file to contain a list of records or a recognized structure, but found type {type(raw_data)}.", file=sys.stderr)
        return None


def analyze_data(data):
    """
    Performs the analysis on the loaded data list to find shared identifiers,
    counts usage per user, and tracks the last connected timestamp.
    'data' is expected to be a flat list of individual log session records.
    """
    print("\nAnalyzing all loaded records for shared identifiers, usage counts, and last connected times...")
    if not data: 
        print("No data available to analyze.")
        return {}, {}, {}
        
    # Structure: identifier -> { userId -> {'count': N, 'last_seen': 'timestamp_str'} }
    hwid_map = defaultdict(lambda: defaultdict(lambda: {'count': 0, 'last_seen': ''}))
    social_club_map = defaultdict(lambda: defaultdict(lambda: {'count': 0, 'last_seen': ''}))
    ip_map = defaultdict(lambda: defaultdict(lambda: {'count': 0, 'last_seen': ''}))

    for record in data: 
        user_id = record.get('userId') 
        hwid = record.get('hwid')
        social_club_id = record.get('socialClubId')
        ip = record.get('ip')
        timestamp = record.get('timestamp') # Get the timestamp from the record

        if user_id is None or timestamp is None: # Timestamp is now essential for 'last_seen'
            continue

        if hwid is not None:
            hwid_map[hwid][user_id]['count'] += 1
            if timestamp > hwid_map[hwid][user_id]['last_seen']: # ISO timestamps can be string compared
                hwid_map[hwid][user_id]['last_seen'] = timestamp
        
        if social_club_id is not None:
            social_club_map[social_club_id][user_id]['count'] += 1
            if timestamp > social_club_map[social_club_id][user_id]['last_seen']:
                social_club_map[social_club_id][user_id]['last_seen'] = timestamp
        
        if ip is not None:
            ip_map[ip][user_id]['count'] += 1
            if timestamp > ip_map[ip][user_id]['last_seen']:
                ip_map[ip][user_id]['last_seen'] = timestamp
            
    # Filter for identifiers shared by more than one distinct user
    shared_hwids = {
        hwid: user_details_map 
        for hwid, user_details_map in hwid_map.items() 
        if len(user_details_map) > 1 
    }
    shared_social_clubs = {
        scid: user_details_map 
        for scid, user_details_map in social_club_map.items() 
        if len(user_details_map) > 1
    }
    shared_ips = {
        ip: user_details_map 
        for ip, user_details_map in ip_map.items() 
        if len(user_details_map) > 1
    }

    print("Analysis complete.")
    return shared_hwids, shared_social_clubs, shared_ips

def print_results(shared_hwids, shared_social_clubs, shared_ips, output_file_handle=None):
    """Prints the analysis results, including usage counts, likely primary user, and last connected time."""
    
    def write_output(message):
        if output_file_handle:
            output_file_handle.write(message + "\n")
        else:
            print(message)

    write_output("\n--- Analysis Results ---")

    if shared_hwids:
        write_output("\nHWIDs shared by multiple users:")
        for hwid, user_details_map in shared_hwids.items(): 
            write_output(f"\n  HWID: {hwid}")
            if not user_details_map:
                write_output("    No user data available for this HWID.")
                continue
            
            # Find the user with the highest count for this HWID
            # Sort by count (descending), then by last_seen (descending, if counts are equal)
            sorted_users = sorted(user_details_map.items(), key=lambda item: (item[1]['count'], item[1]['last_seen']), reverse=True)
            primary_user_id, primary_details = sorted_users[0]
            
            write_output(f"    - Likely primary to User ID: {primary_user_id} (used {primary_details['count']} times, last connected: {primary_details['last_seen']})")
            
            for user_id, details in sorted_users: # Iterate through sorted users
                if user_id != primary_user_id: 
                    write_output(f"    - User ID: {user_id} used {details['count']} times (last connected: {details['last_seen']})")
    else:
        write_output("\nNo HWIDs found shared by multiple users.")

    if shared_social_clubs:
        write_output("\nSocialClub IDs shared by multiple users:")
        for scid, user_details_map in shared_social_clubs.items():
            write_output(f"\n  SocialClub ID: {scid}")
            if not user_details_map:
                write_output("    No user data available for this SocialClub ID.")
                continue

            sorted_users = sorted(user_details_map.items(), key=lambda item: (item[1]['count'], item[1]['last_seen']), reverse=True)
            primary_user_id, primary_details = sorted_users[0]

            write_output(f"    - Likely primary to User ID: {primary_user_id} (used {primary_details['count']} times, last connected: {primary_details['last_seen']})")
            for user_id, details in sorted_users:
                if user_id != primary_user_id:
                    write_output(f"    - User ID: {user_id} used {details['count']} times (last connected: {details['last_seen']})")
    else:
        write_output("\nNo SocialClub IDs found shared by multiple users.")

    if shared_ips:
        write_output("\nIP Addresses shared by multiple users:")
        for ip, user_details_map in shared_ips.items():
            write_output(f"\n  IP Address: {ip}")
            if not user_details_map:
                write_output("    No user data available for this IP Address.")
                continue
                
            sorted_users = sorted(user_details_map.items(), key=lambda item: (item[1]['count'], item[1]['last_seen']), reverse=True)
            primary_user_id, primary_details = sorted_users[0]

            write_output(f"    - Likely primary to User ID: {primary_user_id} (used {primary_details['count']} times, last connected: {primary_details['last_seen']})")
            for user_id, details in sorted_users:
                if user_id != primary_user_id:
                    write_output(f"    - User ID: {user_id} used {details['count']} times (last connected: {details['last_seen']})")
    else:
        write_output("\nNo IP Addresses found shared by multiple users.")
    write_output("\n--- End of Analysis ---")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Load connection logs from a JSON file and analyze for shared identifiers, usage frequency, and last connected times across all users."
    )
    parser.add_argument("filename", help="Path to the JSON file containing connection log records.")
    parser.add_argument("--analyze", action="store_true", help="Perform shared identifier analysis on the loaded data. If not provided, script only loads and counts records.")
    parser.add_argument("--output-analysis", type=str, help="Optional: Path to save the analysis results to a text file.")

    args = parser.parse_args()

    log_data = load_data_from_file(args.filename)

    if log_data is not None: 
        if not log_data: 
            print("Log file loaded, but it resulted in no processable log records.")
            sys.exit(0)
        
        print(f"\n--- Sample of processed log data for analysis (first record if any) ---")
        if log_data:
            try:
                print(json.dumps(log_data[0], indent=2))
            except IndexError:
                print("Log data list is empty after processing.")
        else:
            print("No log data to sample after processing.")

        if args.analyze:
            hwids, scids, ips = analyze_data(log_data)
            
            if args.output_analysis:
                try:
                    with open(args.output_analysis, 'w', encoding='utf-8') as outfile:
                        print_results(hwids, scids, ips, outfile)
                    print(f"\nAnalysis results successfully saved to: {args.output_analysis}")
                except Exception as e:
                    print(f"Error saving analysis results to '{args.output_analysis}': {e}", file=sys.stderr)
            else:
                print_results(hwids, scids, ips)
        else:
            print(f"Data loaded and processed into {len(log_data)} individual log records. Use the --analyze flag to perform shared identifier analysis.")

    else:
        print("Could not load or process data. Processing aborted.", file=sys.stderr)
        sys.exit(1)

    print("\nScript finished.")