# PROCI Workflow

A repository for the systematic extraction, processing, and analysis of peer review data from Crossref to create a novel citation index compliant with the OpenCitations Data Model.

## Quick Overview of the repository:
OpenCitations-Enhanced-Citation-Index/
│
├── data/
│   ├── raw/
│   │   └── lookup.csv
│   │    
│   ├── processed/
│   │   ├── analysis
│   │   ├── compartimentized
│   │   ├── filtered
│   │   ├── non peer
│   │   └── peer
│   │  
│   └── results/
│
├── scripts/
│   ├── extraction/
│   │   ├── PeerExtractor.py
│   │   └── NonPeerExtractor.py
│   ├── processing/
│   │   ├── FilterJoinDeltaDir.py
│   │   └── Compartimentizer.py
│   ├── post_processing/
│   │   └── RDFcreator.py
│   ├── analysis/
│   │   ├── VenueCounter.py
│   │   └── MetaAnalysis.py
│   └── run.py
│
├── notebooks/
│   └── workflow.ipynb
│
├── requirements.txt
├── README.md
└── LICENSE.md


---

## Abstract

This repository provides a step-by-step methodology for extracting, aligning, and analyzing peer review data from Crossref, aimed at enhancing the OpenCitations Index. The workflow encompasses four phases:

1. **Data Gathering**
2. **Data Processing**
3. **Post-Processing**
4. **Data Analysis and Visualization**

The workflow addresses key research questions about citation dynamics and peer review coverage in scholarly metadata.

### Key Highlights:
- **Purpose**: To create a citation index featuring typed citations where a peer review (citing entity) reviews a publication (cited entity).
- **Originality**: Enhances scholarly metadata, enabling better insights into research impact and peer review interactions.

---

## How to Use

### Setup and Installation

1. **Requirements**: Python 3.10 or later.
2. **Clone the Repository**:
   ```bash
   git clone https://github.com/username/OpenCitations-Enhanced-Citation-Index
   cd OpenCitations-Enhanced-Citation-Index

3. **Install Dependencies**:
    ```sh
        pip install -r requirements.txt
    ```
4. **Prepare Data: Download the datasets**:
        Crossref: Crossref Dump (185GB, April 2023)
        OpenCitations Meta: Meta Dump (11GB, April 2024)

### Workflow Execution

1. **Data Gathering**:

Extract data from the Crossref dump:

Peer Reviews:
```sh
    python run.py PeerExtractor <path_to_zip> <output_csv>
```
Non-Peer Reviews:
```sh
    python run.py NonPeerExtractor <path_to_zip> <output_csv>
```
2. **Data Processing**:

Combine datasets and calculate temporal deltas:
```sh
    python run.py FilterJoinDeltaDir \
    --filter_peer_review_dir <peer_dir> \
    --filter_non_peer_review_dir <non_peer_dir> \
    --filter_output_path <output_csv>
```
3. **Post-Processing**:

Split Data into Separate CSVs:
```sh
    python run.py Compartimentizer <input_csv>
```
Generate RDF:
```sh
    python run.py RDF \
      --rdf_input <input_csv> \
      --rdf_output <output_file> \
      --rdf_baseurl <base_url> \
      --rdf_populate_data
```
4. **Data Analysis and Visualization**:

Analyze Top Venues:
    ```sh
    python run.py Venue <input_csv> --venue_output_file <output_csv>
    ```
Cross-Reference Data with OpenCitations Meta:
    ```sh
        python run.py Meta <combined_csv> <meta_zip_file> --meta_mode all --meta_output_file <output_csv>
    ```
Research Questions

- What percentage of Crossref peer reviews are in OpenCitations Meta?
- Which venues receive the most peer reviews?
- How many citations involve resources in both Crossref and Meta?
- What are the time dynamics of peer-reviewed citations?

### Data Details
**Crossref**:

    Size: 185GB (April 2023)
    Structure: Metadata for scholarly publications including DOIs, titles, authors, dates, venues, peer review details.

**OpenCitations Meta**:

    Size: 11GB (April 2024)
    Structure: Citation networks including citing DOI, cited DOI, publication dates, and reviewer information.

## Results:

**Outputs**:

-     CSV Files:
  -       Citations
  -       Provenance
  -       Venues
-     RDF:
  -       Serialized triples in N-Triples format.
**Visualizations**:
-       Bar charts, donut charts, and line graphs for citation statistics and dynamics.

### Repository Structure:

-   **PeerExtractor.py**: Extract peer review data.
-   **NonPeerExtractor.py**: Extract non-peer-reviewed data.
-   **FilterJoinDeltaDir.py**: Combine datasets and compute deltas.
-   **Compartimentizer.py**: Split data into separate CSVs.
-   **RDFcreator.py**: Create RDF graphs from citation data.
-   **VenueCounter.py**: Analyze publication venues.
-   **MetaAnalysis.py**: Compare Crossref with OpenCitations Meta.