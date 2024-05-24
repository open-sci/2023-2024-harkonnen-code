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

from PeerExtractor import PeerExtractor, OciProcess, CSVWriter
from NonPeerExtractor import NonPeerExtractor, CSVWriter
from FilterJoinDeltaDir import Filter, Delta
from Compartimentizer import Compartimentizer
from RDFcreator import PeerReview, populate_data, populate_prov
from VenueCounter import VenueCounter
from MetaAnalysis import MetaAnalysis

# Example of usage?

def parse_args():
    parser = argparse.ArgumentParser(description="Peer Reviews processor")


    # PeerExtractor -- parameters
    # parser = argparse.ArgumentParser(description="Process JSON.gz files in a ZIP and output to CSV.")
    parser.add_argument("zip_filename", help="The input ZIP file containing JSON.gz files.")
    parser.add_argument("output_filenames", help="The output CSV file(s).", nargs='+')
    parser.add_argument("--batch_size", type=int, default=10, help="Number of files to process in each batch.")
    parser.add_argument("--max_files", type=int, help="Maximum number of files to process.")

    # NonPeerExtractor -- parameters  
    # parser = argparse.ArgumentParser(description="Process JSON.gz files in a ZIP and output to CSV.")
    parser.add_argument("zip_filename", help="The input ZIP file containing JSON.gz files.")
    parser.add_argument("output_filenames", help="The output CSV file(s).", nargs='+')
    parser.add_argument("--batch_size", type=int, default=10, help="Number of files to process in each batch.")
    parser.add_argument("--max_files", type=int, help="Maximum number of files to process.")
    
    # FilterJoinDelta -- parameters
    # parser = argparse.ArgumentParser(description="Join peer review and non-peer review DataFrames and calculate delta.")
    parser.add_argument("--peer_review_dir", help="The directory containing the peer review CSV files.", required=True)
    parser.add_argument("--non_peer_review_dir", help="The directory containing the non-peer review CSV files.", required=True)
    parser.add_argument("--output_path", help="The path of the output CSV file.", required=True)    
    
    # Compartimentizer -- parameters
    # parser = argparse.ArgumentParser(description="DataFrame Compartimentizer")
    parser.add_argument("input_path", help="Path to the input CSV file")

    # RDFcreator -- parameters
    # parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('--input', type=str, help='Input CSV file', required=True)
    parser.add_argument('--output', type=str, help='Output file', required=True)
    parser.add_argument('--baseurl', type=str, help='Base URL', required=True)
    parser.add_argument('--data', dest='include_data', action='store_true', help='Include data')
    parser.add_argument('--prov', dest='include_prov', action='store_true', help='Include provenance')
    parser.add_argument('--populate_data', dest='populate_data', action='store_true', help='Populate data')
    parser.add_argument('--populate_prov', dest='populate_prov', action='store_true', help='Populate provenance')
    
    # VenueCounter -- parameters
    # parser = argparse.ArgumentParser(description="Venue Counter")
    parser.add_argument('csv_file', help='Path to the input CSV file')
    parser.add_argument('--top_n', type=int, default=10, help='Number of top venues to display')
    parser.add_argument('--output_file', help='Path to the output CSV file to save results')

    # MetaAnalysis -- parameters
    # parser = argparse.ArgumentParser(description="Meta Analysis")
    parser.add_argument('combined_csv', help='Path to the combined CSV file')
    parser.add_argument('zip_file', help='Path to the OpenAlex zip file')
    parser.add_argument('--mode', choices=['peer', 'article', 'all'], default='all', help='Mode of operation')
    parser.add_argument('--output_file', help='Path to the output CSV file to save counts')

    args = parser.parse_args()
    return args

def main():
    args = parse_args()
    print(args)

    # PeerExtractor
    csv_writer = CSVWriter(args.output_filenames)
    article_processor = PeerExtractor(args.zip_filename, args.batch_size)
    article_processor.process_files(csv_writer, args.max_files)

    for output_filename in args.output_filenames:
        unique_output_filename = output_filename.replace(".csv", "_unique.csv")
        csv_writer.remove_duplicates(output_filename, unique_output_filename)


    # NonPeerExtractor
    csv_writer = CSVWriter(args.output_filenames)
    article_processor = NonPeerExtractor(args.zip_filename, args.batch_size)
    article_processor.process_files(csv_writer, args.max_files)

    
    # FilterJoinDelta
    data_filter = Filter(args.peer_review_dir, args.non_peer_review_dir, args.output_path)
    print(data_filter)
    print(os.listdir(args.peer_review_dir))
    print(os.listdir(args.non_peer_review_dir))
    data_filter = Filter(args.peer_review_dir, args.non_peer_review_dir, args.output_path)
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
    compartimentizer.compartimentizer(args.input_path)

    # RDFCreator
    if args.populate_data:
        populate_data(args.input, args.output, args.baseurl, include_data=args.include_data, include_prov=False)
    elif args.populate_prov:
        populate_prov(args.input, args.output, args.baseurl, include_data=args.include_data, include_prov=args.include_prov)
    else:
        print("No action specified. Use --populate_data or --populate_prov.")

    # VenueCounter
    counter = VenueCounter(args.csv_file)
    top_venues = counter.get_top_venues(args.top_n)
    print(f"Top {args.top_n} venues:")
    print(top_venues)

    if args.output_file:
        counter.save_to_csv(args.output_file)

    # MetaAnalysis
    analysis = MetaAnalysis(args.combined_csv)

    peer_count = article_count = None
    if args.mode == 'peer' or args.mode == 'all':
        peer_count = analysis.get_peer_review_count(args.zip_file)
        print(f"Number of peer reviews: {peer_count}")
    if args.mode == 'article' or args.mode == 'all':
        article_count = analysis.get_article_count(args.zip_file)
        print(f"Number of articles: {article_count}")

    if args.output_file:
        analysis.save_counts_to_csv(args.output_file, peer_count, article_count)


if __name__ == '__main__':
    main()