import polars as pl
import argparse

class Compartimentizer:
    def __init__(self, columns_to_drop, columns_to_drop1, columns_to_drop2, output_path, output_path1, output_path2):

        self.columns_to_drop = columns_to_drop
        self.columns_to_drop1 = columns_to_drop1
        self.columns_to_drop2 = columns_to_drop2
        self.output_path = output_path
        self.output_path1 = output_path1
        self.output_path2 = output_path2
        pass


    def compartimentizer(self, path):
        df = pl.read_csv(path)
        df1 = df.clone()
        df2 = df.clone()
        df = df.drop(self.columns_to_drop)
        df1 = df1.drop(self.columns_to_drop1)
        df2 = df2.drop(self.columns_to_drop2) 
        df.write_csv(self.output_path)
        df1.write_csv(self.output_path1)
        df2.write_csv(self.output_path2)
       



def main():
    parser = argparse.ArgumentParser(description="DataFrame Compartimentizer")
    parser.add_argument("compart_input_path", help="Path to the input CSV file")
    args = parser.parse_args()
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

if __name__ == "__main__":
    main()