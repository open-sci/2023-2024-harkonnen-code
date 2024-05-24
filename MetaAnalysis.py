import pandas as pd
import zipfile
import argparse
from tqdm import tqdm

class MetaAnalysis:
    def __init__(self, combined_csv_path):
        self.combined_csv_path = combined_csv_path

    def extract_doi_from_meta(self, zip_file_path, output_file_path):
        with open(output_file_path, 'w', encoding='utf-8') as out_file:
            out_file.write('DOI\n')

        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            file_list = [file for file in zip_ref.namelist() if file.endswith('.csv')]
            for file_name in tqdm(file_list, desc="Extracting DOIs"):
                with zip_ref.open(file_name) as csv_file:
                    for chunk in pd.read_csv(csv_file, chunksize=10000):
                        chunk_dois = chunk.iloc[:, 0].apply(self.extract_doi_from_text)
                        chunk_dois = chunk_dois[chunk_dois != '']
                        chunk_dois.to_csv(output_file_path, mode='a', index=False, header=False)

    def extract_doi_from_text(self, text):
        if 'doi:' in text:
            start_index = text.find('doi:') + len('doi:')
            end_index = text.find(' ', start_index)
            if end_index == -1:
                end_index = len(text)
            doi = text[start_index:end_index]
            if ',' in doi:
                doi = f'"{doi}"'
            return doi
        return ''

    def count_rows(self, file_path):
        return sum(1 for line in open(file_path, 'r', encoding='utf-8')) - 1

    def create_peer_review_and_article_lists(self, meta_file_path):
        combined_df = pd.read_csv(self.combined_csv_path)
        doi_peer_set = set(combined_df['citing_doi'])
        doi_article_set = set(combined_df['cited_doi'])

        chunk_size = 10000
        with open('meta_peer.csv', 'w', encoding='utf-8') as peer_file, open('meta_article.csv', 'w', encoding='utf-8') as article_file:
            peer_file.write('DOI\n')
            article_file.write('DOI\n')

            for chunk in tqdm(pd.read_csv(meta_file_path, chunksize=chunk_size), desc="Processing DOIs"):
                meta_peer_list = chunk.iloc[:, 0][chunk.iloc[:, 0].isin(doi_peer_set)]
                meta_article_list = chunk.iloc[:, 0][chunk.iloc[:, 0].isin(doi_article_set)]

                meta_peer_list.to_csv(peer_file, mode='a', index=False, header=False)
                meta_article_list.to_csv(article_file, mode='a', index=False, header=False)

    def drop_duplicates_and_save(self, input_file, output_file):
        df = pd.read_csv(input_file)
        df.drop_duplicates(inplace=True)
        df.to_csv(output_file, index=False)

    def get_peer_review_count(self, zip_file_path):
        self.extract_doi_from_meta(zip_file_path, "meta_doi.csv")
        self.create_peer_review_and_article_lists("meta_doi.csv")
        self.drop_duplicates_and_save('meta_peer.csv', 'meta_peer_cleaned.csv')
        return self.count_rows('meta_peer_cleaned.csv')

    def get_article_count(self, zip_file_path):
        self.extract_doi_from_meta(zip_file_path, "meta_doi.csv")
        self.create_peer_review_and_article_lists("meta_doi.csv")
        self.drop_duplicates_and_save('meta_article.csv', 'meta_article_cleaned.csv')
        return self.count_rows('meta_article_cleaned.csv')

    def save_counts_to_csv(self, output_file_path, peer_count, article_count):
        results = {
            'Peer Reviews': [peer_count],
            'Articles': [article_count]
        }
        df = pd.DataFrame(results)
        df.to_csv(output_file_path, index=False)
        print(f"Counts saved to {output_file_path}")

# if __name__ == "__main__":
#     parser = argparse.ArgumentParser(description="Meta Analysis")
#     parser.add_argument('combined_csv', help='Path to the combined CSV file')
#     parser.add_argument('zip_file', help='Path to the OpenAlex zip file')
#     parser.add_argument('--mode', choices=['peer', 'article', 'all'], default='all', help='Mode of operation')
#     parser.add_argument('--output_file', help='Path to the output CSV file to save counts')

#     args = parser.parse_args()
#     analysis = MetaAnalysis(args.combined_csv)

#     peer_count = article_count = None
#     if args.mode == 'peer' or args.mode == 'all':
#         peer_count = analysis.get_peer_review_count(args.zip_file)
#         print(f"Number of peer reviews: {peer_count}")
#     if args.mode == 'article' or args.mode == 'all':
#         article_count = analysis.get_article_count(args.zip_file)
#         print(f"Number of articles: {article_count}")

#     if args.output_file:
#         analysis.save_counts_to_csv(args.output_file, peer_count, article_count)







