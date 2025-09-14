

def format_numbers_from_file(input_filename="message.txt", output_filename=None):
    try:
        with open(input_filename, 'r') as f_in:
            # Read all lines and strip newline characters
            numbers = [line.strip() for line in f_in.readlines()]
            numbers.sort()

        # Join the numbers with a space
        formatted_string = " ".join(numbers)

        if output_filename:
            with open(output_filename, 'w') as f_out:
                f_out.write(formatted_string)
            print(f"Formatted numbers written to {output_filename}")
        else:
            print(formatted_string)

    except FileNotFoundError:
        print(f"Error: The file '{input_filename}' was not found.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    print("\nOutputting to file 'output.txt':")
    format_numbers_from_file("message.txt", "output.txt")
