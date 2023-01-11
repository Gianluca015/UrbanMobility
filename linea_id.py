from neo4j import GraphDatabase
import osmnx as ox
import overpy
import sys
import os
import geojson
import geopandas as gpd
import pandas as pd
import numpy as np
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
    
    # creo le relazioni "linea_id" per collegare i nodi fermata fra di loro
    def create_linea_id(self, linea, stop, stop1):
        """import nodes in the graph."""
        with self.driver.session() as session:
            result = session.write_transaction(self._create_linea_id, linea, stop, stop1)
            return result

    @staticmethod
    def _create_linea_id(tx, linea, stop, stop1):
        result = tx.run("""
                           MATCH (l:Linea)
                           WHERE l.id=$linea
                           MATCH (f:Fermata)
                           WHERE f.id=$stop1
                           MATCH (f1:Fermata)
                           WHERE f1.id=$stop
                           MATCH (l)-[s:STOP_IN]->(f)
                           MATCH (l)-[s1:STOP_IN]->(f1)
                           MERGE (f)-[p:LINEA_ID]->(f1)
                           ON CREATE SET p.duration=(duration.between(localtime(s1.run),localtime(s.run))).minutes;
                       """, linea=linea, stop=stop, stop1=stop1)
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
    path = connection.get_path()[0][0] + '\\' + connection.get_import_folder_name()[0][0] + '\\linee_fer_orario.json' 

    f = open(path)
    linee_fermate = json.load(f)
    new_lf=dict()
    id_fermata=list()
    id_linea=list()
    sequenza=list()
    corsa=list()
    for f in linee_fermate["features"]:
        id_fermata.append(f["properties"]["stop_id"])
        id_linea.append(f["properties"]["id"])
        sequenza.append(f["properties"]["sequenza"])
        corsa.append(f["properties"]["corsa"])
    new_lf["stop_id"]=id_fermata
    new_lf["id"]=id_linea
    new_lf["sequenza"]=sequenza
    new_lf["corsa"]=corsa
    df_lf = pd.DataFrame.from_dict(new_lf)
    df_lf["sequenza"]=df_lf["sequenza"].astype(int)
    df_lf_ordered = df_lf.sort_values(by=["sequenza"])
    for l in df_lf.id.unique():
        df_lf_l=df_lf_ordered.stop_id[df_lf_ordered.id==l]
        k=0
        j=0
        for n in df_lf_l:
            if k==0:
                j=n
                k=k+1
            else:
                connection.create_linea_id(int(l),int(df_lf["stop_id"][j==df_lf["stop_id"]].unique()),int(df_lf["stop_id"][n==df_lf["stop_id"]].unique()))
                j=n
                k=k+1
    print("relationships: done")

    connection.close()

    return 0


if __name__ == "__main__":
    main()
