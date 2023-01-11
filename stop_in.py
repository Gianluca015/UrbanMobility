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

class App:
    # creo una nuova istanza del driver per stabilire una connessione con l'istanza Neo4j creata
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()
    
    # creo le relazioni "stop_in" per collegare i nodi linea ai nodi fermata, chiamando il file linee_fer_orario.json passato da input
    def create_stop_in(self, file):
        """import nodes in the graph."""
        with self.driver.session() as session:
            result = session.write_transaction(self._create_stop_in, file)
            return result

    @staticmethod
    def _create_stop_in(tx, file):
        result = tx.run("""
                           CALL apoc.load.json($file) YIELD value 
                           UNWIND value.features AS features
                           UNWIND features.properties AS properties
                           MATCH (l:Linea)
                           WHERE properties.id=l.id
                           MATCH (f:Fermata)
                           WHERE properties.stop_id=f.id
                           MERGE (l)-[r:STOP_IN]->(f)
                           ON CREATE SET r.sequence=properties.sequenza, r.run=localtime(properties.corsa);
                       """, file=file)
        return result.values()

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
    return parser

def main(args=None):
    #parsing input parameters
    argParser = add_options()
    # retrieving arguments
    options = argParser.parse_args(args=args)

    # connecting to the neo4j instance
    connection = App(options.neo4jURL, options.neo4juser, options.neo4jpwd)
    
    # create relationships between lines and stop nodes
    connection.create_stop_in(options.file_name)
    print("relationships: done")

    connection.close()

    return 0


if __name__ == "__main__":
    main()
