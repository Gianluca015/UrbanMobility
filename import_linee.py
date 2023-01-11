from neo4j import GraphDatabase
import osmnx as ox
import overpy
import sys
import os
import geojson
import geopandas as gpd
import json
import argparse
import fiona
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from shapely import wkt
from shapely.ops import unary_union

class App:
    # creo una nuova istanza del driver per stabilire una connessione con l'istanza Neo4j creata
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def get_path(self):
        """gets the path of the neo4j instance"""

        with self.driver.session() as session:
            result = session.write_transaction(self._get_path)
            return result

    @staticmethod
    def _get_path(tx):
        result = tx.run("""
                        Call dbms.listConfig() yield name,value where name = 'dbms.directories.neo4j_home' return value;
                    """)
        return result.values()
        
    def get_import_folder_name(self):
        """gets the path of the import folder of the neo4j instance"""

        with self.driver.session() as session:
            result = session.write_transaction(self._get_import_folder_name)
            return result

    @staticmethod
    def _get_import_folder_name(tx):
        result = tx.run("""
                        Call dbms.listConfig() yield name,value where name = 'dbms.directories.import' return value;
                    """)
        return result.values()

    # importo i nodi delle linee, chiamando il file linee.json passato da input
    def import_node(self, file):
        """import nodes in the graph."""
        with self.driver.session() as session:
            result = session.write_transaction(self._import_node, file)
            return result

    @staticmethod
    def _import_node(tx, file):
        result = tx.run("""
                           CALL apoc.load.json($file) YIELD value 
                           UNWIND value.features AS features
                           UNWIND features.properties AS properties
                           MERGE (l:Linea {nome_linea: properties.name})
                           ON CREATE SET l.id=properties.id, l.from=properties.from, l.name=properties.name, 
                           l.network=properties.network, l.operator=properties.operator, l.to=properties.to,
                           l.geometry=properties.geom;
                       """, file=file)
        return result.values()
    
    # importo i nodi linea, sfruttando le coordinate spaziali con Neo4j Spatial
    def import_lines_in_spatial_layer(self):
        """import lines nodes on a Neo4j Spatial Layer"""
        with self.driver.session() as session:
            result = session.write_transaction(self._import_lines_in_spatial_layer)
            return result

    @staticmethod
    def _import_lines_in_spatial_layer(tx):
        result = tx.run("""    
                            MATCH (l:Linea) WITH collect(l) AS lines UNWIND lines AS n 
                            CALL spatial.addNode('spatial', n) yield node return node
                        """)
        return result.values

def add_options():
    parser = argparse.ArgumentParser(description='Inserimento linee nel grafo.')
    parser.add_argument('--neo4jURL', '-n', dest='neo4jURL', type=str,
                        help="""Insert the address of the local neo4j instance. For example: neo4j://localhost:7687""",
                        required=True)
    parser.add_argument('--neo4juser', '-u', dest='neo4juser', type=str,
                        help="""Insert the name of the user of the local neo4j instance.""",
                        required=True)
    parser.add_argument('--neo4jpwd', '-p', dest='neo4jpwd', type=str,
                        help="""Insert the password of the local neo4j instance.""",
                        required=True)
    parser.add_argument('--nameFile', '-f', dest='file_name', type=str,
                        help="""Insert the name of the .json file.""",
                        required=True)
    parser.add_argument('--nameFile2', '-d', dest='file_name2', type=str,
                        help="""Insert the name of the new .json file.""",
                        required=True)
    return parser

def main(args=None):
    #parsing input parameters
    argParser = add_options()
    # retrieving arguments
    options = argParser.parse_args(args=args)

    # connecting to the neo4j instance
    connection = App(options.neo4jURL, options.neo4juser, options.neo4jpwd)
    path = connection.get_path()[0][0] + '\\' + connection.get_import_folder_name()[0][0] + '\\linee.json' 

    f = open(path)
    linee = json.load(f)
    j=0
    for i in linee["features"]:
        ty=i["geometry"]["type"].upper()
        geom=ty
        geom=geom+"("
        for l in i["geometry"]["coordinates"]:
            geom=geom+"("
            for p in l[:-1]:
                geom=geom+str(p[0])+" "+str(p[1])+","
            geom=geom+str(l[-1][0])+" "+str(l[-1][1])
            geom=geom+")"
            if l != i["geometry"]["coordinates"][-1]:
                geom=geom+","
        geom=geom+")"
        linee["features"][j]["properties"]["geom"]=geom
        j=j+1 
    print("geom: done")
    with open(options.file_name2, "w") as f2:
        json.dump(linee, f2, indent=2)
    print("save: done")

    #import lines nodes
    connection.import_node(options.file_name2)
    print("import lines node: done")

    
    #import lines nodes on a Neo4j Spatial Layer
    connection.import_lines_in_spatial_layer()
    print("Import the lines in the spatial layer: done")

    connection.close()

    return 0


if __name__ == "__main__":
    main()
