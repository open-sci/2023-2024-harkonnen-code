import pandas as pd
import argparse

class VenueCounter:
    def __init__(self, csv_file_path):
        self.csv_file_path = csv_file_path

    def count_venues(self):
        dtypes = {
            'cited_issn': 'str',
            'cited_venue': 'str'
        }

        df = pd.read_csv(self.csv_file_path, dtype=dtypes, usecols=['cited_issn', 'cited_venue'])
        df = df.fillna('')

        comma_rows = df[df['cited_issn'].str.contains(',')]
        comma_rows = comma_rows.copy()
        comma_rows[['issn1', 'issn2']] = comma_rows['cited_issn'].str.split(',', expand=True)

        grouped_comma = comma_rows.groupby(['issn1', 'issn2', 'cited_venue']).size().reset_index(name='count')

        no_comma_rows = df[~df['cited_issn'].str.contains(',')]
        no_comma_groups = no_comma_rows.groupby(['cited_issn', 'cited_venue']).size().reset_index(name='count')
        no_comma_groups = no_comma_groups.rename(columns={'cited_issn': 'issn1'})
        no_comma_groups['issn2'] = None

        empty_issn_rows = df[df['cited_issn'] == '']
        empty_issn_groups = empty_issn_rows.groupby('cited_venue').size().reset_index(name='count')
        empty_issn_groups['issn1'] = ''
        empty_issn_groups['issn2'] = None

        final_group = pd.concat([grouped_comma, no_comma_groups, empty_issn_groups], ignore_index=True)

        final_group = final_group.drop_duplicates()

        return final_group

    def get_top_venues(self, n=10):
        grouped_df_comma = self.count_venues()
        top_venues = grouped_df_comma.sort_values(by='count', ascending=False).head(n)
        return top_venues

    def save_to_csv(self, output_file_path):
        grouped_df_comma = self.count_venues()
        grouped_df_comma.to_csv(output_file_path, index=False)
        print(f"Results saved to {output_file_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Venue Counter")
    parser.add_argument('venue_csv_file', help='Path to the input CSV file')
    parser.add_argument('--venue_top_n', type=int, default=10, help='Number of top venues to display')
    parser.add_argument('--venue_output_file', help='Path to the output CSV file to save results')

    args = parser.parse_args()
    counter = VenueCounter(args.venue_csv_file)
    top_venues = counter.get_top_venues(args.venue_top_n)
    print(f"Top {args.venue_top_n} venues:")
    print(top_venues)

    if args.venue_output_file:
        counter.save_to_csv(args.venue_output_file)
