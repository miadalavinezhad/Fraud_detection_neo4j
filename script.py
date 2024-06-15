import numpy as np
import pandas as pd
from neo4j import GraphDatabase


# URI examples: "neo4j://localhost", "neo4j+s://xxx.databases.neo4j.io"
URI = "neo4j+s://39c51803.databases.neo4j.io"
AUTH = ("neo4j", "DkrQqzei-tEYNqm1EJlGHzXfz6NqDJpO2G5Gn3qNx6Y")

with GraphDatabase.driver(URI, auth=AUTH) as driver:
    driver.verify_connectivity()


