import pyautogui
import time # It's good practice to import time explicitly for sleep

def write():
    shared = [100, 7553, 8769, 1103, 918, 914, 6591, 5909, 7067, 749, 471, 7591, 7658, 6178, 7863, 2401, 6146, 8089, 1537, 1316, 1140, 2467, 6302, 1586, 4195, 523, 524, 2401, 2878, 2402, 3656, 4086, 4086, 4513, 3137, 5731, 5734, 9439, 560, 9746, 9536, 7295, 6335, 6766, 6520, 7082, 8393]
    # Consider a slightly longer initial delay to switch to the target window
    print("Script will start in 5 seconds. Please focus the target window.")
    time.sleep(5)

    for i in shared:
        # Add an interval between keystrokes for more reliability
        pyautogui.write(f'/warn {i} Account Sharing', interval=0.02) 
        
        time.sleep(0.75)  # Increased delay after writing the message
        pyautogui.press('enter')
        print(f"Warned: {i}")
        time.sleep(0.5) # Add a delay before processing the next item

if __name__ == "__main__":
    print("Starting to warn accounts for account sharing...")
    write()
    print("Finished warning accounts.")