import numpy as np
import pandas as pd
import csv
from datetime import datetime
from neo4j import GraphDatabase


# URI examples: "neo4j://localhost", "neo4j+s://xxx.databases.neo4j.io"
URI = "neo4j+s://39c51803.databases.neo4j.io"
AUTH = ("neo4j", "DkrQqzei-tEYNqm1EJlGHzXfz6NqDJpO2G5Gn3qNx6Y")


with open("C:/Users/Asus/Documents/University/New-Gen-Project/Scripts/data_sets_50/customer.csv", 'r') as f:
    reader = csv.reader(f)
    next(reader) # skip header row
    customer = [row for row in reader]


with open("C:/Users/Asus/Documents/University/New-Gen-Project/Scripts/data_sets_50/terminal.csv", 'r') as f:
    reader = csv.reader(f)
    next(reader) # skip header row
    terminal = [row for row in reader]


with open("C:/Users/Asus/Documents/University/New-Gen-Project/Scripts/data_sets_50/transaction.csv", 'r') as f:
    reader = csv.reader(f)
    next(reader) # skip header row
    transaction = [row for row in reader]

#############################################################

# FROM 'https://media.githubusercontent.com/media/miadalavinezhad/Fraud_detection_neo4j/main/data_sets_50/customer.csv' AS row
# FROM 'https://media.githubusercontent.com/media/miadalavinezhad/Fraud_detection_neo4j/main/data_sets_50/terminal.csv' AS row
# FROM 'https://media.githubusercontent.com/media/miadalavinezhad/Fraud_detection_neo4j/main/data_sets_50/transaction.csv' AS row


load_customer = """
    LOAD CSV
        WITH HEADERS
        FROM 'https://media.githubusercontent.com/media/miadalavinezhad/Fraud_detection_neo4j/main/data_sets_50/customer.csv' AS row
    CREATE (c:Customer {customer_id: row.CUSTOMER_ID, x_customer_id: row.x_customer_id, y_customer_id:  row.y_customer_id, mean_amount: row.mean_amount, std_amount: row.std_amount, mean_nb_tx_per_day: row.mean_nb_tx_per_day, available_terminals: row.available_terminals, nb_terminals: row.nb_terminals})
    
    
"""

load_terminal = """
        LOAD CSV
            WITH HEADERS
            FROM 'https://media.githubusercontent.com/media/miadalavinezhad/Fraud_detection_neo4j/main/data_sets_50/terminal.csv' AS row
        CREATE (t:Terminal {terminal_id: row.TERMINAL_ID, 
                x_terminal_id: row.x_terminal_id, y_terminal_id: row.y_terminal_id})
"""

load_transaction_node = """
        LOAD CSV
           WITH HEADERS
           FROM 'https://media.githubusercontent.com/media/miadalavinezhad/Fraud_detection_neo4j/main/data_sets_50/transaction.csv' AS row
        MERGE (tx:Transaction {transaction_id: row.TRANSACTION_ID, transaction_datetime: row.TX_DATETIME, transaction_amount: row.TX_AMOUNT})                                
"""

load_transaction = """
        LOAD CSV    
            WITH HEADERS
            FROM 'https://media.githubusercontent.com/media/miadalavinezhad/Fraud_detection_neo4j/main/data_sets_50/transaction.csv' AS row
        MATCH (c:Customer {customer_id: row.CUSTOMER_ID}), (t:Terminal {terminal_id: row.TERMINAL_ID}), (tx:Transaction {transation_id: row.TRANSACTION_ID})
        CREATE (c)-[:MADE]->(tx)
        CREATE (t)-[:PROCESS]->(tx)
        MERGE (c)-[:USE]->(t)
"""

customer_query = '''
                CREATE (c:Customer {customer_id: $customer_id,
                                    c.x_customer_id = $x_customer_id,
                                    c.y_customer_id = $y_customer_id,
                                    c.mean_amount = $mean_amount,
                                    c.std_amount = $std_amount,
                                    c.mean_nb_tx_per_day = $mean_nb_tx_per_day,
                                    c.available_terminals = $available_terminals,
                                    c.nb_terminals = $nb_terminals})
                '''


terminal_query = '''
                CREATE (t:Terminal {terminal_id: $terminal_id, t.x_terminal_id = $t.x_terminal_id,  t.y_terminal_id = $y_terminal_id})
                '''


transaction_query = '''
                CREATE (tx:Transaction {transaction_id: $transaction_id, tx.transaction_datetime = datetime($transaction_datetime),  tx.transaction_amount = $transaction_amount})
                MATCH (c:Customer {customer_id: $customer_id}), (t:Terminal {terminal_id: $terminal_id}), (tx:Transaction {transaction_id: $transaction_id})
                CREATE (c)-[:MADE]->(tx)
                CREATE (t)-[:PROCESS]->(tx)
                CREATE (c)-[:USE]->(t)
                '''


URI = "bolt://localhost:7687/"
AUTH = ("neo4j", "12345678")

# Session

with GraphDatabase.driver(URI, auth=AUTH) as driver:
    driver.verify_connectivity()

    with driver.session() as session:
        session.run(load_customer).data()
        print('customer')
        session.run(load_terminal).data()
        print('terminal')
        session.run(load_transaction_node).data()
        print('tran node')
        session.run(load_transaction).data()
        print('tran')


"""
    with driver.session() as session:

        # Create Customer nodes
        for row in customer:
            params = {'customer_id': int(row[0]), 'x_customer_id': float(row[1]),
                      'y_customer_id': float(row[2]), 'mean_amount': float(row[3]), 
                      'std_amount': float(row[4]), 'mean_nb_tx_per_day': float(row[5]),
                      'available_terminals': row[6],'nb_terminals': int(row[7])}
            
            session.run(customer_query, params)


        # Create Terminal nodes
        for row in terminal:
            params = {'terminal_id': int(row[0]), 'x_terminal_id': float(row[1]),
                      'y_terminal_id': float(row[2])}

            session.run(terminal_query, params)


        # Create Transaction nodes and relationships
        for row in transaction:
            params = {'transaction_id': int(row[1]), 'transaction_datetime':datetime.strptime(row[2], "%Y-%m-%d %H:%M:%S"),
                      'transaction_amount': float(row[5]),
                      'customer_id': int(row[3]), 'terminal_id': int(row[4])}

            session.run(transaction_query, params)

"""          
