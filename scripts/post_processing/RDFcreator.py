from rdflib import Graph, RDF, RDFS, XSD, URIRef, Literal
from dateutil.relativedelta import relativedelta
from dateutil.parser import parse
from datetime import datetime
import csv
from io import StringIO
from urllib.parse import quote
import os
import errno
import argparse

class PeerReview(object):
    # predicates citation
    __cito_base = "http://purl.org/spar/cito/"
    _reviews = URIRef(__cito_base + "reviews") 
    _citation = URIRef(__cito_base + "Citation")
    _has_citation_creation_date = URIRef(__cito_base + "hasCitationCreationDate")
    _has_citation_time_span = URIRef(__cito_base + "hasCitationTimeSpan")
    _has_citing_entity = URIRef(__cito_base + "hasCitingEntity")
    _has_cited_entity = URIRef(__cito_base + "hasCitedEntity")
    _has_citation_characterization = URIRef(__cito_base + "hasCitationCharacterisation")

    # predicates provenance
    __prov_base = "http://www.w3.org/ns/prov#"
    _was_attributed_to = URIRef(__prov_base + "wasAttributedTo")
    _had_primary_source = URIRef(__prov_base + "hadPrimarySource")
    _generated_at_time = URIRef(__prov_base + "generatedAtTime")

    # init
    def __init__(self, oci, citing_url=None, cited_url=None, time_span=None, citing_date=None, prov_agent_url=None, source=None, prov_date=None):
        self.oci = oci[4:]
        self.citing_url = citing_url
        self.citing_date = citing_date
        self.cited_url = cited_url
        self.time_span = time_span
        self.prov_agent_url = prov_agent_url
        self.source = source
        self.prov_date = prov_date

    def get_peer_review_rdf(self, baseurl, include_data=True, include_prov=True):
        peer_review_graph = Graph()
        citation_corpus_id = "ci/" + self.oci
        citation = URIRef(baseurl + citation_corpus_id)

        if include_data:
            if self.citing_url:
                citing_br = URIRef(self.citing_url)
                peer_review_graph.add((citation, self._has_citing_entity, citing_br))

            if self.cited_url:
                cited_br = URIRef(self.cited_url)
                peer_review_graph.add((citation, self._has_cited_entity, cited_br))

            peer_review_graph.add((citation, RDF.type, self._citation))
            peer_review_graph.add((citation, self._has_citation_characterization, self._reviews))

            if self.citing_date is not None:
                if PeerReview.contains_days(self.citing_date):
                    xsd_type = XSD.date
                elif PeerReview.contains_months(self.citing_date):
                    xsd_type = XSD.gYearMonth
                else:
                    xsd_type = XSD.gYear

                peer_review_graph.add((citation, self._has_citation_creation_date,
                                    Literal(self.citing_date, datatype=xsd_type, normalize=False)))
                if self.time_span is not None:
                    peer_review_graph.add((citation, self._has_citation_time_span,
                                        Literal(self.time_span, datatype=XSD.duration)))

        if include_prov:
            if self.prov_agent_url:
                peer_review_graph.add((citation, self._was_attributed_to, URIRef(self.prov_agent_url)))
            if self.source:
                peer_review_graph.add((citation, self._had_primary_source, URIRef(self.source)))
            if self.prov_date:
                peer_review_graph.add((citation, self._generated_at_time, Literal(self.prov_date, datatype=XSD.dateTime)))

        return peer_review_graph
    
    @staticmethod
    def contains_years(date):
        return date is not None and len(date) >= 4

    @staticmethod
    def contains_months(date):
        return date is not None and len(date) >= 7

    @staticmethod
    def contains_days(date):
        return date is not None and len(date) >= 10

def populate_data(csv_file, output_file, base_url, include_data=True, include_prov=False):
    with open(csv_file, 'r') as file:
        reader = csv.DictReader(file, delimiter=',')
        for row in reader:
            oci = row['oci']
            citing_url = row['citing_url'] 
            cited_url = row['cited_url']
            citing_date = row['citing_date'] 
            time_span = row['time_span']

            citation = PeerReview(oci,
                                  citing_url=citing_url,
                                  cited_url=cited_url,
                                  time_span=time_span,
                                  citing_date=citing_date)

            g = citation.get_peer_review_rdf(base_url, include_data=include_data, include_prov=include_prov)

            with open(output_file, 'a', newline='') as f:
                f.write(g.serialize(format='nt'))

def populate_prov(csv_file, output_file, base_url, include_data=False, include_prov=True):
    block_txt = ''
    with open(csv_file, 'r') as file:
        reader = csv.DictReader(file, delimiter=',')
        for row in reader:
            oci = row['oci']
            agent_url = row['prov_agent_url']
            prov_source = row['source']
            prov_date = row['prov_date']

            citation = PeerReview(oci,
                                  prov_agent_url=agent_url,
                                  source=prov_source,
                                  prov_date=prov_date)

            g = citation.get_peer_review_rdf(base_url, include_data=include_data, include_prov=include_prov)
            block_txt += g.serialize(format='nt')

    if block_txt:
        with open(output_file, 'a', newline='') as f:
            f.write(block_txt)

def main():
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('--rdf_input', type=str, help='Input CSV file', required=True)
    parser.add_argument('--rdf_output', type=str, help='Output file', required=True)
    parser.add_argument('--rdf_baseurl', type=str, help='Base URL', required=True)
    parser.add_argument('--rdf_data', dest='include_data', action='store_true', help='Include data')
    parser.add_argument('--rdf_prov', dest='include_prov', action='store_true', help='Include provenance')
    parser.add_argument('--rdf_populate_data', dest='populate_data', action='store_true', help='Populate data')
    parser.add_argument('--rdf_populate_prov', dest='populate_prov', action='store_true', help='Populate provenance')

    args = parser.parse_args()

    if args.rdf_populate_data:
        populate_data(args.rdf_input, args.rdf_output, args.rdf_baseurl, include_data=args.rdf_include_data, include_prov=False)
    elif args.rdf_populate_prov:
        populate_prov(args.rdf_input, args.rdf_output, args.rdf_baseurl, include_data=args.rdf_include_data, include_prov=args.rdf_include_prov)
    else:
        print("No action specified. Use --populate_data or --populate_prov.")

if __name__ == "__main__":
    main()
