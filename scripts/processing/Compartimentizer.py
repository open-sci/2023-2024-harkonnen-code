import polars as pl
import argparse
import os

class Compartimentizer:
    def __init__(self, columns_to_drop, columns_to_drop1, columns_to_drop2, output_path, output_path1, output_path2):
        self.columns_to_drop = columns_to_drop
        self.columns_to_drop1 = columns_to_drop1
        self.columns_to_drop2 = columns_to_drop2
        self.output_path = output_path
        self.output_path1 = output_path1
        self.output_path2 = output_path2

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
        print(f"Files saved to: {self.output_path}, {self.output_path1}, {self.output_path2}")

def main():
    parser = argparse.ArgumentParser(description="DataFrame Compartimentizer")
    parser.add_argument("compart_input_path", help="Path to the input CSV file")
    parser.add_argument("--output_dir", help="Directory to save the output CSV files", default="data/processed/compartimentized")
    args = parser.parse_args()

    # Assicurati che la directory di output esista
    if not os.path.exists(args.output_dir):
        os.makedirs(args.output_dir)

    # Estrai il nome base del file di input
    input_basename = os.path.basename(args.compart_input_path)
    input_name_no_ext = os.path.splitext(input_basename)[0]

    # Definisci i percorsi di output
    output_path = os.path.join(args.output_dir, f"{input_name_no_ext}_Citation.csv")
    output_path1 = os.path.join(args.output_dir, f"{input_name_no_ext}_Provenance.csv")
    output_path2 = os.path.join(args.output_dir, f"{input_name_no_ext}_Venue.csv")

    # Definizione delle colonne da eliminare
    columns_to_drop = ["cited_issn", "cited_venue", "prov_agent", "source", "prov_date"]
    columns_to_drop1 = ["citing_doi", "cited_doi", "citing_date", "cited_date", "citing_url", "cited_url", "cited_issn", "cited_venue", "cited_date", "time_span"]
    columns_to_drop2 = ["oci", "citing_doi", "citing_date", "cited_date", "citing_url", "cited_url", "cited_date", "time_span", "prov_agent", "source", "prov_date", "time_span"]

    # Crea l'istanza del Compartimentizer con i percorsi di output definiti
    compartimentizer = Compartimentizer(columns_to_drop=columns_to_drop, 
                                        columns_to_drop1=columns_to_drop1, 
                                        columns_to_drop2=columns_to_drop2, 
                                        output_path=output_path,
                                        output_path1=output_path1, 
                                        output_path2=output_path2)

    compartimentizer.compartimentizer(args.compart_input_path)

if __name__ == "__main__":
    main()
