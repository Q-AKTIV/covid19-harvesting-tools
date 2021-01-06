#!/usr/bin/env python3
# -*- coding: utf8 -*-

"""
File: harvest_mesh
Author: Steffen Trog
Email: shtrog@gmail.com
Github: https://github.com/knorpelsenf
Description: Extract concept hierarchy from mesh.nt so every (sub)concept refers to a list of broader concepts.
"""
import argparse
import json
from tqdm import tqdm

from rdflib import Graph, URIRef
from collections import defaultdict

broaderDescriptor = URIRef("http://id.nlm.nih.gov/mesh/vocab#broaderDescriptor")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('file_in',
			help="The path to the N-triple file")
    parser.add_argument('file_out',
                        help="Store output json in this file")
    
    args = parser.parse_args()
    print("Loading", args.file_in)
    g = Graph()
    g.parse(args.file_in, format='nt')
    print("#Statements:", len(g))
    broader = defaultdict(list)
    for s,p,o in tqdm(g.triples( (None, broaderDescriptor, None) )):
        # preferredLabel returns list of (property, literal) pairs
        s_label = g.preferredLabel(s, lang='en')[0][1].value
        o_label = g.preferredLabel(o, lang='en')[0][1].value
        print(s_label, '->', o_label)
        broader[s_label].append(o_label)

    print("Found broader relationships for", len(broader), "concepts")
    
    print("Writing JSON...")
    with open(args.file_out, "w") as file:
        json.dump(broader, file)

    print("Success!")

if __name__ == '__main__':
    main()

