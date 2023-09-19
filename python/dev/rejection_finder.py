import argparse
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
from time import perf_counter
import os

def process_lines(batch):
    rows = []
    tag_11_to_100 = {}
    for line in batch:
        tag_58_value = None
        tag_11_value = None
        tag_100_value = None
        tags = line.split('')

        for tag in tags:
            if tag.startswith("11="):
                tag_11_value = tag[3:]
            elif tag.startswith("58="):
                tag_58_value = tag[3:]
            elif tag.startswith("100="):
                tag_100_value = tag[4:]

        if tag_11_value and tag_58_value:
            rows.append({'Order ID': tag_11_value, 'Rejection Reason': tag_58_value})

        if tag_11_value and tag_100_value:
            tag_11_to_100[tag_11_value] = tag_100_value

    return rows, tag_11_to_100

def extract_tag_values(file_path):
    start_time = perf_counter()
    print("Reading file...")
    with open(file_path, 'r') as file:
        lines = file.readlines()
    print("File read completed.")

    batch_size = 100000
    batches = [lines[i:i + batch_size] for i in range(0, len(lines), batch_size)]

    all_rows = []
    tag_11_to_100 = {}

    print("Processing lines...")
    with ThreadPoolExecutor(max_workers=8) as executor:
        results = executor.map(process_lines, batches)

    for rows, partial_tag_11_to_100 in results:
        all_rows.extend(rows)
        tag_11_to_100.update(partial_tag_11_to_100)

    print(f"Processing lines took {perf_counter() - start_time:.2f} seconds.")

    print("Building DataFrame...")
    df = pd.DataFrame(all_rows)

    print("Filtering DataFrame on unique tag 58 values...")
    df.drop_duplicates(subset=['Rejection Reason'], inplace=True)

    print("Adding Exchange info to DataFrame...")
    df['Exchange'] = df['Order ID'].apply(lambda x: tag_11_to_100.get(x, 'N/A'))

    print(f"Total time taken: {perf_counter() - start_time:.2f} seconds.")
    
    filename = os.path.basename(file_path).split('.')[0]
    output_filename = f'output_{filename}.csv'

    print(f"Saving DataFrame to CSV as {output_filename}...")
    df.to_csv(output_filename, index=False)
    print("CSV saved.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process log file.')
    parser.add_argument('file_path', type=str, help='The path to the log file to be processed.')

    args = parser.parse_args()
    extract_tag_values(args.file_path)
