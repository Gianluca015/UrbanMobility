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

    # importo i nodi delle fermate, chiamando il file fer.json passato da input
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
                           MERGE (n:Fermata {nome_fermata: properties.name})
                           ON CREATE SET n.id=properties.id, n.nome=properties.name, n.lat=properties.latitude, 
                           n.lon=properties.longitude, n.strada_id=properties.strada_id, n.l_array=properties.l_array,
                           n.geometry=properties.geom, n.location = point({latitudine: tofloat(n.lat), longitudine: tofloat(n.lon)});
                       """, file=file)
        return result.values()

# importo i nodi linea, sfruttando le coordinate spaziali con Neo4j Spatial
    def import_stop_in_spatial_layer(self):
        """import lines nodes on a Neo4j Spatial Layer"""
        with self.driver.session() as session:
            result = session.write_transaction(self._import_stop_in_spatial_layer)
            return result

    @staticmethod
    def _import_stop_in_spatial_layer(tx):
        tx.run("""
                    CALL spatial.addWKTLayer('spatial', 'geometry')
                """)
        print("spatial layer: created")
        result = tx.run("""    
                            MATCH (f:Fermata) WITH collect(f) AS stop UNWIND stop AS s 
                            CALL spatial.addNode('spatial', s) yield node return node
                        """)
        return result.values

def add_options():
    parser = argparse.ArgumentParser(description='Inserimento fermate nel grafo.')
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
    path = connection.get_path()[0][0] + '\\' + connection.get_import_folder_name()[0][0] + '\\fer.json' 

    f = open(path)
    fermate = json.load(f)
    j=0
    for i in fermate["features"]:
        geom=i["geometry"]["type"].upper()+"("+str(i["geometry"]["coordinates"][0])+" "+str(i["geometry"]["coordinates"][1])+")"
        fermate["features"][j]["properties"]["geom"]=geom
        j=j+1
    print("geom: done")
    with open(options.file_name2, "w") as f2:
        json.dump(fermate, f2, indent=2)
    print("save: done")
    
    #creating the graph
    connection.import_node(options.file_name2)
    print("import node: done")

    #import stop nodes on a Neo4j Spatial Layer
    connection.import_stop_in_spatial_layer()
    print("Import the stop nodes in the spatial layer: done")

    connection.close()

    return 0


if __name__ == "__main__":
    main()
