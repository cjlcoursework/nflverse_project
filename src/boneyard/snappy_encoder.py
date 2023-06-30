import snappy


def encode_to_snappy(input_file, output_file):
    with open(input_file, 'rb') as f_in:
        input_data = f_in.read()

    snappy_data = snappy.compress(input_data)

    with open(output_file, 'wb') as f_out:
        f_out.write(snappy_data)


def decode_snappy_file(input_file, output_file):
    with open(input_file, 'rb') as f_in:
        snappy_data = f_in.read()

    decoded_data = snappy.decompress(snappy_data)

    with open(output_file, 'wb') as f_out:
        f_out.write(decoded_data)


# Example usage
input_file_path = 'input.txt'       # Path to the input text file
snappy_file_path = 'output.snappy'  # Path to the Snappy-compressed file
decoded_file_path = 'decoded.txt'   # Path to save the decoded text file

# Encode the input file to a Snappy-compressed file
encode_to_snappy(input_file_path, snappy_file_path)
print('File encoded to Snappy-compressed format.')

# Decode the Snappy-compressed file to a text file
decode_snappy_file(snappy_file_path, decoded_file_path)
print('File decoded from Snappy-compressed format.')

