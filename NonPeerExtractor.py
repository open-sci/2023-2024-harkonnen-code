import zipfile
import gzip
import json
import csv
from concurrent.futures import ThreadPoolExecutor, as_completed
import argparse
from tqdm import tqdm

class NonPeerExtractor:
    def __init__(self, zip_filename, batch_size=10, max_workers = 2):
        self.zip_filename = zip_filename
        self.batch_size = batch_size
        self.max_workers = max_workers

    def process_files(self, csv_writer, max_files=None):
        with zipfile.ZipFile(self.zip_filename, 'r') as zip_file:
            file_infos = [file_info for file_info in zip_file.infolist() if file_info.filename.endswith(".json.gz")]
            
            if max_files:
                file_infos = file_infos[:max_files]
            
            for batch in self.batch(file_infos, self.batch_size):
                non_peer_review_items = self.process_batch(zip_file, batch)
                csv_writer.write_to_csv(non_peer_review_items)

    def batch(self, iterable, n=1):
        length = len(iterable)
        for ndx in range(0, length, n):
            yield iterable[ndx:min(ndx + n, length)]

    def process_batch(self, zip_file, batch):
        non_peer_review_items = []
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [executor.submit(self.process_file, zip_file, file_info) for file_info in batch]
            for future in tqdm(as_completed(futures), total=len(futures), desc="Processing batch"):
                non_peer_review_items.extend(future.result())
        return non_peer_review_items

    def process_file(self, zip_file, file_info):
        with zip_file.open(file_info) as compressed_file:
            compressed_data = compressed_file.read()
            return self.process_json_data(compressed_data)

    def process_json_data(self, compressed_data):
        decompressed_data = gzip.decompress(compressed_data)
        decoded_data = decompressed_data.decode('utf-8')
        
        try:
            json_data = json.loads(decoded_data)
        except json.JSONDecodeError:
            print("Decoding error because of: ", decoded_data)
            return []

        if isinstance(json_data, dict) and 'items' in json_data:
            items = json_data['items']
        elif isinstance(json_data, list):
            items = json_data
        else:
            print("JSON structure not recognized: ", json_data)
            return []

        non_peer_review_items = [item for item in items if item.get('type') != 'peer-review']
        return non_peer_review_items

class CSVWriter:
    def __init__(self, output_filenames):
        if isinstance(output_filenames, str):
            self.output_filenames = [output_filenames]
        else:
            self.output_filenames = output_filenames
        self.header_written = False

    def write_to_csv(self, non_peer_review_items):
        for output_filename in self.output_filenames:
            with open(output_filename, 'a', newline='', encoding='utf-8') as output_file:
                fieldnames = ['cited_doi', 'cited_url', 'cited_issn', 'cited_venue', 'cited_date']
                writer = csv.DictWriter(output_file, fieldnames=fieldnames)
                if output_file.tell() == 0:
                    writer.writeheader()

                for element in non_peer_review_items:
                    doi_a = element.get("DOI")
                    url_a = element.get("URL")
                    date_non_peer_review = str(element.get("created", {}).get("date-time", ""))[:10]
                    issn = ', '.join(element.get('ISSN', []))
                    container_title = ', '.join(element.get('container-title', []))
                    if doi_a and url_a:
                        writer.writerow({
                            "cited_doi": doi_a,
                            "cited_url": url_a,
                            "cited_issn": issn,
                            "cited_venue": container_title,
                            "cited_date": date_non_peer_review
                        })
            print("Batch saved to", output_filename)

def main():
    parser = argparse.ArgumentParser(description="Process JSON.gz files in a ZIP and output to CSV.")
    parser.add_argument("non_peer_zip_filename", help="The input ZIP file containing JSON.gz files.")
    parser.add_argument("non_peer_output_filenames", help="The output CSV file(s).", nargs='+')
    parser.add_argument("--non_peer_batch_size", type=int, default=10, help="Number of files to process in each batch.")
    parser.add_argument("--non_peer_max_files", type=int, help="Maximum number of files to process.")
    parser.add_argument("--non_peer_max_workers", type=int, default=2, help="Number of maximum worker threads.")


    args = parser.parse_args()

    csv_writer = CSVWriter(args.non_peer_output_filenames)
    article_processor = NonPeerExtractor(args.non_peer_zip_filename, args.non_peer_batch_size)
    article_processor.process_files(csv_writer, args.non_peer_max_files)

if __name__ == "__main__":
    main()