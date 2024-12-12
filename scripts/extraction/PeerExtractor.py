import zipfile
import gzip
import json
import csv
import os
import errno
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import polars as pl

LOOKUP_CSV = '../data/raw/lookup.csv'
CROSSREF_CODE = '020'

class PeerExtractor:
    def __init__(self, zip_filename, batch_size=10, max_workers = 2):
        self.zip_filename = zip_filename
        self.batch_size = batch_size
        self.max_workers = max_workers

    def process_files(self, csv_writer, max_files=None):
        print("Using PeerExtractor")
        with zipfile.ZipFile(self.zip_filename, 'r') as zip_file:
            file_infos = [file_info for file_info in zip_file.infolist() if file_info.filename.endswith(".json.gz")]
            
            if max_files:
                file_infos = file_infos[:max_files]
            
            for batch in self.batch(file_infos, self.batch_size):
                peer_review_items = self.process_batch(zip_file, batch)
                csv_writer.write_to_csv(peer_review_items)

    def batch(self, iterable, n=1):
        length = len(iterable)
        for ndx in range(0, length, n):
            yield iterable[ndx:min(ndx + n, length)]

    def process_batch(self, zip_file, batch):
        peer_review_items = []
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [executor.submit(self.process_file, zip_file, file_info) for file_info in batch]
            for future in tqdm(as_completed(futures), total=len(futures), desc="Processing batch"):
                peer_review_items.extend(future.result())
        return peer_review_items

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
            print("Decoding errore because of: ", decoded_data)
            return []

        if isinstance(json_data, dict) and 'items' in json_data:
            items = json_data['items']
        elif isinstance(json_data, list):
            items = json_data
        else:
            print("Json structure not recognized: ", json_data)
            return []

        peer_review_items = [item for item in items if item.get('type') == 'peer-review']
        return peer_review_items

class OciProcess:
    def __init__(self, lookup_csv=LOOKUP_CSV, crossref_code=CROSSREF_CODE):
        self.lookup_code = 0
        self.lookup_dic = {}
        self.LOOKUP_CSV = lookup_csv
        self.CROSSREF_CODE = crossref_code
        self.init_lookup_dic()

    def init_lookup_dic(self):
        with open(self.LOOKUP_CSV, 'r', encoding='utf-8') as lookupcsv:
            lookupcsv_reader = csv.DictReader(lookupcsv)
            code = -1
            for row in lookupcsv_reader:
                if row['c'] not in self.lookup_dic:
                    self.lookup_dic[row['c']] = row['code']
                    code = int(row['code'])
            self.lookup_code = code

    def calc_next_lookup_code(self):
        rem = self.lookup_code % 100
        newcode = self.lookup_code + 1
        if rem == 89:
            newcode = (self.lookup_code // 100 + 1) * 100
        self.lookup_code = newcode

    def update_lookup(self, c):
        if c not in self.lookup_dic:
            self.calc_next_lookup_code()
            code = str(self.lookup_code).zfill(2)
            self.lookup_dic[c] = code
            self.write_txtblock_on_csv(self.LOOKUP_CSV, '\n"%s","%s"' % (c, code))

    def write_txtblock_on_csv(self, csv_path, block_txt):
        directory = os.path.dirname(csv_path)
        if directory and not os.path.exists(directory):
            try:
                os.makedirs(directory)
            except OSError as exc:
                if exc.errno != errno.EEXIST:
                    raise
        with open(csv_path, 'a', newline='', encoding='utf-8') as csvfile:
            csvfile.write(block_txt)

    def convert_doi_to_ci(self, doi_str):
        return self.CROSSREF_CODE + self.match_str_to_lookup(doi_str)

    def match_str_to_lookup(self, str_val):
        ci_str = ""
        str_noprefix = str_val[3:]
        for c in str_noprefix:
            if c not in self.lookup_dic:
                self.update_lookup(c)
            ci_str += str(self.lookup_dic[c])
        return ci_str

class CSVWriterPeer:
    def __init__(self, output_filenames):
        if isinstance(output_filenames, str):
            self.output_filenames = [output_filenames]
        else:
            self.output_filenames = output_filenames
        self.header_written = False

    def write_to_csv(self, peer_review_items):
        for output_filename in self.output_filenames:
            # Creazione automatica della directory
            output_dir = os.path.dirname(output_filename)
            if output_dir and not os.path.exists(output_dir):
                print(f"Creating directory for output: {output_dir}")
                os.makedirs(output_dir)
            
            with open(output_filename, 'a', newline='', encoding='utf-8') as output_file:
                fieldnames = ["oci", "citing_doi", "cited_doi", "citing_date", "citing_url", "author_info"]
                writer = csv.DictWriter(output_file, fieldnames=fieldnames)
                if output_file.tell() == 0:
                    writer.writeheader()

                oci_processor = OciProcess()

                for element in peer_review_items:
                    for i in element.get("relation", {}).get("is-review-of", []):
                        doi_p = element["DOI"]
                        doi_a = i.get("id", "")
                        url_p = element["URL"]
                        citing_entity_local_id = oci_processor.convert_doi_to_ci(doi_p)
                        cited_entity_local_id = oci_processor.convert_doi_to_ci(doi_a)
                        oci = "oci:" + citing_entity_local_id + "-" + cited_entity_local_id
                        date_peer_review = str(element["created"]["date-time"])[:10]

                        # Estrazione informazioni sugli autori
                        author_list = []
                        for author in element.get("author", []):
                            family_name = author.get("family", "").strip()
                            given_name = author.get("given", "").strip()
                            
                            if family_name and given_name:
                                author_info = f"{family_name}, {given_name}"
                            else:
                                author_info = family_name  # Solo il cognome se manca il nome

                            orcid = author.get("ORCID", "")
                            author_list.append(f"{author_info} (ORCID: {orcid})" if orcid else author_info)

                        author_info_str = "; ".join(author_list)

                        if doi_p and doi_a:
                            writer.writerow({
                                "oci": oci,
                                "citing_doi": doi_p,
                                "cited_doi": doi_a,
                                "citing_date": date_peer_review,
                                "citing_url": url_p,
                                "author_info": author_info_str
                            })
            print("peer items saved to", output_filename)

    def remove_duplicates(self, input_filename, output_filename):
        df = pl.read_csv(input_filename)
        df_unique = df.unique(subset=['oci'])
        df_unique.write_csv(output_filename)
        print(f"Unique peer items saved to {output_filename}")