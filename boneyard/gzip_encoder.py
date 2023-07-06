import gzip

def uncompress_gzip(input_file, output_file):
    with gzip.open(input_file, 'rb') as f_in:
        with open(output_file, 'wb') as f_out:
            f_out.write(f_in.read())

# Example usage
compressed_file_path = 'compressed.gz'  # Path to the compressed Gzip file
output_file_path = 'uncompressed.txt'   # Path to save the uncompressed file

# Uncompress the Gzip file
uncompress_gzip(compressed_file_path, output_file_path)
print('File uncompressed successfully.')