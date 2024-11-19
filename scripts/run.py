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

from extraction.PeerExtractor import PeerExtractor, OciProcess, CSVWriterPeer
from extraction.NonPeerExtractor import NonPeerExtractor, CSVWriterNonPeer
from processing.FilterJoinDeltaDir import Filter, Delta
from processing.Compartimentizer import Compartimentizer
from post_processing.RDFcreator import PeerReview, populate_data, populate_prov
from analysis.VenueCounter import VenueCounter
from analysis.MetaAnalysis import MetaAnalysis

# Example of usage?

def parse_args():
    parser = argparse.ArgumentParser(description="Main program")
    subparsers = parser.add_subparsers(dest='command', required=True)

    # PeerExtractor -- parameters
    peer_parser = subparsers.add_parser('PeerExtractor', help='Process JSON.gz files in a ZIP and output to CSV.')
    peer_parser.add_argument("peer_zip_filename", help="The input ZIP file containing JSON.gz files.")
    peer_parser.add_argument("--peer_output_file", help="Path to save the output CSV file", default="../data/processed/peer/peer_results.csv")
    peer_parser.add_argument("--peer_batch_size", type=int, default=10, help="Number of files to process in each batch.")
    peer_parser.add_argument("--peer_max_files", type=int, help="Maximum number of files to process.")
    peer_parser.add_argument("--peer_max_workers", type=int, default=2, help="Number of maximum worker threads.")

    # NonPeerExtractor -- parameters
    non_peer_parser = subparsers.add_parser('NonPeerExtractor', help='Process JSON.gz files in a ZIP and output to CSV.')
    non_peer_parser.add_argument("non_peer_zip_filename", help="The input ZIP file containing JSON.gz files.")
    non_peer_parser.add_argument("--non_peer_output_file", help="Path to save the output CSV file", default="../data/processed/non_peer/non_peer_results.csv")
    non_peer_parser.add_argument("--non_peer_batch_size", type=int, default=10, help="Number of files to process in each batch.")
    non_peer_parser.add_argument("--non_peer_max_files", type=int, help="Maximum number of files to process.")
    non_peer_parser.add_argument("--non_peer_max_workers", type=int, default=2, help="Number of maximum worker threads.")

    
    # FilterJoinDelta -- parameters
    filter_parser = subparsers.add_parser("FilterJoinDeltaDir", help="Join peer review and non-peer review DataFrames and calculate delta")
    filter_parser.add_argument("--filter_peer_review_dir", help="The directory containing the peer review CSV files.", required=True)
    filter_parser.add_argument("--filter_non_peer_review_dir", help="The directory containing the non-peer review CSV files.", required=True)
    filter_parser.add_argument("--filter_output_path", help="Directory to save the output CSV file", default="../data/processed/filtered/results.csv")  
    
    # Compartimentizer -- parameters
    compart_parser = subparsers.add_parser("Compartimentizer", help="DataFrame Compartimentizer")
    compart_parser.add_argument("compart_input_path", help="Path to the input CSV file")
    compart_parser.add_argument("--output_dir", help="Directory to save the output CSV files", default="../data/processed/compartimentized")
    
    # RDFcreator -- parameters
    rdf_parser = subparsers.add_parser("RDF", help="Process some integers.")
    rdf_parser.add_argument('--rdf_input', type=str, help='Input CSV file', required=True)
    rdf_parser.add_argument('--rdf_output', type=str, help='Output file', required=True)
    rdf_parser.add_argument('--rdf_baseurl', type=str, help='Base URL', required=True)
    rdf_parser.add_argument('--rdf_data', dest='include_data', action='store_true', help='Include data')
    rdf_parser.add_argument('--rdf_prov', dest='include_prov', action='store_true', help='Include provenance')
    rdf_parser.add_argument('--rdf_populate_data', dest='populate_data', action='store_true', help='Populate data')
    rdf_parser.add_argument('--rdf_populate_prov', dest='populate_prov', action='store_true', help='Populate provenance')
    
    # VenueCounter -- parameters
    venue_parser = subparsers.add_parser("Venue", help="Path to the input CSV file")
    venue_parser.add_argument('venue_csv_file', help='Path to the input CSV file')
    venue_parser.add_argument('--venue_top_n', type=int, default=10, help='Number of top venues to display')
    venue_parser.add_argument('--venue_output_file', help='Path to the output CSV file to save results')

    # MetaAnalysis -- parameters
    meta_parser = subparsers.add_parser("Meta", help="Meta Analysis")
    meta_parser.add_argument('meta_combined_csv', help='Path to the combined CSV file')
    meta_parser.add_argument('meta_zip_file', help='Path to the OpenAlex zip file')
    meta_parser.add_argument('--meta_mode', choices=['peer', 'article', 'all'], default='all', help='Mode of operation')
    meta_parser.add_argument('--meta_output_file', help='Path to the output CSV file to save counts')

    args = parser.parse_args()
    return args

def main():
    args = parse_args()
    print(args)
    print(f"Running command: {args.command}")

    if args.command == 'PeerExtractor':
        input_basename = os.path.splitext(os.path.basename(args.peer_zip_filename))[0]
        
        peer_output_file = os.path.join(
            os.path.dirname(args.peer_output_file),
            f"{input_basename}_peer_results.csv"
        )

        csv_writer = CSVWriterPeer(peer_output_file)
        article_processor = PeerExtractor(args.peer_zip_filename, args.peer_batch_size)
        article_processor.process_files(csv_writer, args.peer_max_files)

        unique_output_filename = peer_output_file.replace(".csv", "_unique.csv")
        if os.path.isfile(peer_output_file):  
            csv_writer.remove_duplicates(peer_output_file, unique_output_filename)
        else:
            print(f"Errore: Il file {peer_output_file} non esiste o non è un file.")

    if args.command == "NonPeerExtractor":
        input_basename = os.path.splitext(os.path.basename(args.non_peer_zip_filename))[0]
    
        non_peer_output_file = os.path.join(
            os.path.dirname(args.non_peer_output_file),
            f"{input_basename}_non_peer_results.csv"
        )

        csv_writer = CSVWriterNonPeer(non_peer_output_file)
        article_processor = NonPeerExtractor(args.non_peer_zip_filename, args.non_peer_batch_size)
        article_processor.process_files(csv_writer, args.non_peer_max_files)

    # FilterJoinDelta
    if args.command == "FilterJoinDeltaDir":
        data_filter = Filter(args.filter_peer_review_dir, args.filter_non_peer_review_dir, args.filter_output_path)
        print(data_filter)
        print(os.listdir(args.filter_peer_review_dir))
        print(os.listdir(args.filter_non_peer_review_dir))
        data_filter = Filter(args.filter_peer_review_dir, args.filter_non_peer_review_dir, args.filter_output_path)
        concatenated_peer_df = data_filter.read_and_concatenate_dataframes(args.filter_peer_review_dir)
        concatenated_non_peer_df = data_filter.read_and_concatenate_dataframes(args.filter_non_peer_review_dir)
        data_filter.validate_dataframes(concatenated_peer_df, concatenated_non_peer_df)
        joined_df = data_filter.join_dataframes(concatenated_peer_df, concatenated_non_peer_df)
        joined_df_with_provenance = data_filter.add_provenance(joined_df)
        delta_calculator = Delta(joined_df_with_provenance)
        delta_calculator.add_delta_column()
        delta_calculator.save_csv(args.filter_output_path)

    # Compartimentizer
    if args.command == "Compartimentizer":
        columns_to_drop = ["cited_issn", "cited_venue", "prov_agent", "source", "prov_date"]
        columns_to_drop1 = ["citing_doi", "cited_doi", "citing_date", "cited_date", "citing_url", "cited_url", "cited_issn", "cited_venue", "cited_date", "time_span"]
        columns_to_drop2 = ["oci", "citing_doi", "citing_date", "cited_date", "citing_url", "cited_url", "cited_date", "time_span", "prov_agent", "source", "prov_date", "time_span"]

        if not os.path.exists(args.output_dir):
            os.makedirs(args.output_dir)

        output_path = os.path.join(args.output_dir, "Citation.csv")
        output_path1 = os.path.join(args.output_dir, "Provenance.csv")
        output_path2 = os.path.join(args.output_dir, "Venue.csv")

        compartimentizer = Compartimentizer(
            columns_to_drop=columns_to_drop,
            columns_to_drop1=columns_to_drop1,
            columns_to_drop2=columns_to_drop2,
            output_path=output_path,
            output_path1=output_path1,
            output_path2=output_path2
        )

        print(f"Saving CSVs as {output_path}, {output_path1}, and {output_path2}")
        compartimentizer.compartimentizer(args.compart_input_path)

    # RDFCreator
    if args.command == "RDF":
        if args.rdf_populate_data:
            populate_data(args.rdf_input, args.rdf_output, args.rdf_baseurl, include_data=args.rdf_include_data, include_prov=False)
        elif args.rdf_populate_prov:
            populate_prov(args.rdf_input, args.rdf_output, args.rdf_baseurl, include_data=args.rdf_include_data, include_prov=args.rdf_include_prov)
        else:
            print("No action specified. Use --populate_data or --populate_prov.")

    # VenueCounter
    if args.command == "Venue":
        counter = VenueCounter(args.venue_csv_file)
        top_venues = counter.get_top_venues(args.venue_top_n)
        print(f"Top {args.venue_top_n} venues:")
        print(top_venues)

        if args.venue_output_file:
            counter.save_to_csv(args.venue_output_file)

    # MetaAnalysis
    if args.command == "Meta":
        
        analysis = MetaAnalysis(args.meta_combined_csv)

        peer_count = article_count = None
        if args.meta_mode == 'peer' or args.meta_mode == 'all':
            peer_count = analysis.get_peer_review_count(args.meta_zip_file)
            print(f"Number of peer reviews: {peer_count}")
        if args.meta_mode == 'article' or args.meta_mode == 'all':
            article_count = analysis.get_article_count(args.meta_zip_file)
            print(f"Number of articles: {article_count}")

        if args.output_file:
            analysis.save_counts_to_csv(args.meta_output_file, peer_count, article_count)


if __name__ == '__main__':
    main()