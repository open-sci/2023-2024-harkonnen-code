### RUN ALL WORKFLOW

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
import pandas as pd
from datetime import datetime
import pytz
from dateutil.parser import parse
from dateutil.relativedelta import relativedelta
from rdflib import Graph, RDF, RDFS, XSD, URIRef, Literal
from io import StringIO
from urllib.parse import quote

from PeerExtractor import PeerExtractor, OciProcess, CSVWriter
from NonPeerExtractor import NonPeerExtractor, CSVWriter
from FilterJoinDeltaDir import Filter, Delta
from Compartimentizer import Compartimentizer
from RDFcreator import PeerReview, populate_data, populate_prov
from VenueCounter import VenueCounter
from MetaAnalysis import MetaAnalysis

# Example of usage
# python run.py \
#     --zip_filename input_zip_file.zip \
#     --peer_output_filenames peer_output_1.csv peer_output_2.csv \
#     --peer_batch_size 10 \
#     --peer_max_files 100 \
#     --max_workers 4 \
#     --non_peer_output_filenames non_peer_output_1.csv non_peer_output_2.csv \
#     --non_peer_batch_size 10 \
#     --non_peer_max_files 100 \
#     --peer_review_dir peer_review_dir \
#     --non_peer_review_dir non_peer_review_dir \
#     --filter_output_path filter_output.csv \
#     --compart_input_path compart_input_file.csv \
#     --rdf_input rdf_input_file.csv \
#     --rdf_output rdf_output.rdf \
#     --baseurl http://example.org/ \
#     --populate_data \
#     --venue_csv_file venue_csv_file.csv \
#     --top_n 10 \
#     --venue_output_file venue_output.csv \
#     --combined_csv combined_csv_file.csv \
#     --meta_zip_file input_zip_file.zip \
#     --mode all \
#     --meta_output_file meta_output.csv


def parse_args():
    parser = argparse.ArgumentParser(description="Peer Reviews processor")


    # PeerExtractor -- parameters
    parser.add_argument("--zip_filename", help="The input ZIP file containing JSON.gz files.")
    parser.add_argument("--peer_output_filenames", help="The output CSV file(s).", nargs='+')
    parser.add_argument("--peer_batch_size", type=int, default=10, help="Number of files to process in each batch.")
    parser.add_argument("--peer_max_files", type=int, help="Maximum number of files to process.")
    parser.add_argument("--max_workers", type=int, default=2, help="Number of maximum worker threads.")

    # NonPeerExtractor -- parameters  
    parser.add_argument("--zip_filename", help="The input ZIP file containing JSON.gz files.")
    parser.add_argument("--non_peer_output_filenames", help="The output CSV file(s).", nargs='+')
    parser.add_argument("--non_peer_batch_size", type=int, default=10, help="Number of files to process in each batch.")
    parser.add_argument("--non_peer_max_files", type=int, help="Maximum number of files to process.")
    parser.add_argument("--max_workers", type=int, default=2, help="Number of maximum worker threads.")
    
    # FilterJoinDelta -- parameters
    parser.add_argument("--peer_review_dir", help="The directory containing the peer review CSV files.", required=True)
    parser.add_argument("--non_peer_review_dir", help="The directory containing the non-peer review CSV files.", required=True)
    parser.add_argument("--filter_output_path", help="The path of the output CSV file.", required=True)    
    
    # Compartimentizer -- parameters
    parser.add_argument("--compart_input_path", help="Path to the input CSV file")

    # RDFcreator -- parameters
    parser.add_argument('--rdf_input', type=str, help='Input CSV file', required=True)
    parser.add_argument('--rdf_output', type=str, help='Output file', required=True)
    parser.add_argument('--baseurl', type=str, help='Base URL', required=True)
    parser.add_argument('--data', dest='include_data', action='store_true', help='Include data')
    parser.add_argument('--prov', dest='include_prov', action='store_true', help='Include provenance')
    parser.add_argument('--populate_data', dest='populate_data', action='store_true', help='Populate data')
    parser.add_argument('--populate_prov', dest='populate_prov', action='store_true', help='Populate provenance')
    
    # VenueCounter -- parameters
    parser.add_argument('--venue_csv_file', help='Path to the input CSV file')
    parser.add_argument('--top_n', type=int, default=10, help='Number of top venues to display')
    parser.add_argument('--venue_output_file', help='Path to the output CSV file to save results')

    # MetaAnalysis -- parameters
    parser.add_argument('--combined_csv', help='Path to the combined CSV file')
    parser.add_argument('--meta_zip_file', help='Path to the OpenAlex zip file')
    parser.add_argument('--mode', choices=['peer', 'article', 'all'], default='all', help='Mode of operation')
    parser.add_argument('--meta_output_file', help='Path to the output CSV file to save counts')

    args = parser.parse_args()
    return args

def main():
    args = parse_args()
    print(args)

    # PeerExtractor
    csv_writer = CSVWriter(args.peer_output_filenames)
    article_processor = PeerExtractor(args.zip_filename, args.peer_batch_size, args.max_workers)
    article_processor.process_files(csv_writer, args.peer_max_files)

    for output_filename in args.peer_output_filenames:
        unique_output_filename = output_filename.replace(".csv", "_unique.csv")
        csv_writer.remove_duplicates(output_filename, unique_output_filename)


    # NonPeerExtractor
    csv_writer = CSVWriter(args.non_peer_output_filenames)
    article_processor = NonPeerExtractor(args.zip_filename, args.non_peer_batch_size, args.max_workers)
    article_processor.process_files(csv_writer, args.non_peer_max_files)

    
    # FilterJoinDelta
    data_filter = Filter(args.peer_review_dir, args.non_peer_review_dir, args.filter_output_path)
    print(data_filter)
    print(os.listdir(args.peer_review_dir))
    print(os.listdir(args.non_peer_review_dir))
    data_filter = Filter(args.peer_review_dir, args.non_peer_review_dir, args.filter_output_path)
    concatenated_peer_df = data_filter.read_and_concatenate_dataframes(args.peer_review_dir)
    concatenated_non_peer_df = data_filter.read_and_concatenate_dataframes(args.non_peer_review_dir)
    data_filter.validate_dataframes(concatenated_peer_df, concatenated_non_peer_df)
    joined_df = data_filter.join_dataframes(concatenated_peer_df, concatenated_non_peer_df)
    joined_df_with_provenance = data_filter.add_provenance(joined_df)
    delta_calculator = Delta(joined_df_with_provenance)
    delta_calculator.add_delta_column()
    delta_calculator.save_csv(args.output_path)

    # Compartimentizer
    columns_to_drop = ["cited_issn", "cited_venue", "prov_agent", "source", "prov_date"]
    columns_to_drop1 = ["citing_doi", "cited_doi", "citing_date", "cited_date", "citing_url", "cited_url", "cited_issn", "cited_venue", "cited_date", "time_span"]
    columns_to_drop2 = ["oci", "citing_doi", "citing_date", "cited_date", "citing_url", "cited_url", "cited_date", "time_span", "prov_agent", "source", "prov_date", "time_span"]
    output_path="Citation.csv",
    output_path1="Provenance.csv", 
    output_path2="Venue.csv"
    compartimentizer = Compartimentizer(columns_to_drop=columns_to_drop, 
                                        columns_to_drop1=columns_to_drop1, 
                                        columns_to_drop2=columns_to_drop2, 
                                        output_path=output_path,
                                        output_path1=output_path1, 
                                        output_path2=output_path2)
    print(f"Saving CSVs as {output_path}, {output_path1} and {output_path2}")
    compartimentizer.compartimentizer(args.compart_input_path)

    # RDFCreator
    if args.populate_data:
        populate_data(args.rdf_input, args.rdf_output, args.baseurl, include_data=args.include_data, include_prov=False)
    elif args.populate_prov:
        populate_prov(args.rdf_input, args.rdf_output, args.baseurl, include_data=args.include_data, include_prov=args.include_prov)
    else:
        print("No action specified. Use --populate_data or --populate_prov.")

    # VenueCounter
    counter = VenueCounter(args.venue_csv_file)
    top_venues = counter.get_top_venues(args.top_n)
    print(f"Top {args.top_n} venues:")
    print(top_venues)

    if args.output_file:
        counter.save_to_csv(args.venue_output_file)

    # MetaAnalysis
    analysis = MetaAnalysis(args.combined_csv)

    peer_count = article_count = None
    if args.mode == 'peer' or args.mode == 'all':
        peer_count = analysis.get_peer_review_count(args.meta_zip_file)
        print(f"Number of peer reviews: {peer_count}")
    if args.mode == 'article' or args.mode == 'all':
        article_count = analysis.get_article_count(args.meta_zip_file)
        print(f"Number of articles: {article_count}")

    if args.output_file:
        analysis.save_counts_to_csv(args.meta_output_file, peer_count, article_count)


if __name__ == '__main__':
    main()