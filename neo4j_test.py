from neo4j import GraphDatabase
import time
import statistics
from utils import display_results
import config

def run_queries_and_analyze():
    print("===== Neo4j Query Performance Analysis =====")
    # Connection details
    neo4j_config = {
        'uri': config.NEO4J_URI,
        'user': config.NEO4J_USER,
        'password': config.NEO4J_PASSWORD
    }
    
    # Define three Cypher queries to execute
    queries = [
        """
        MATCH (a:Account)-[:HAS_TRANSACTION]->(t:Transaction)
        OPTIONAL MATCH (a)-[:HAS_DISP]->(d:Disp)-[:BELONGS_TO]->(c:Client)
        WITH a, COUNT(DISTINCT t) AS transaction_count, 
            SUM(toFloat(t.amount)) AS total_amount, 
            AVG(toFloat(t.amount)) AS avg_transaction_amount,
            MAX(toFloat(t.amount)) AS max_transaction_amount
        WHERE transaction_count > 100
        RETURN a.account_id, transaction_count, total_amount, avg_transaction_amount, max_transaction_amount
        ORDER BY total_amount DESC
        LIMIT 100;
        """,# Query 1
        """
        MATCH (a:Account)-[:HAS_TRANSACTION]->(t:Transaction)
        WITH a, t, toFloat(t.amount) AS trans_amount
        WITH a, COLLECT(trans_amount) AS transactions
        WITH a, transactions, 
            REDUCE(sum = 0.0, amount IN transactions | sum + amount) AS total_amount,
            REDUCE(count = 0, amount IN transactions | count + 1) AS total_transactions
        WITH a, total_amount / total_transactions AS avg_amount, transactions
        UNWIND transactions AS trans_amount
        WITH a, trans_amount, avg_amount
        WHERE trans_amount > 1.5 * avg_amount
        WITH a, COUNT(trans_amount) AS high_value_count, AVG(trans_amount) AS high_value_avg, MAX(trans_amount) AS max_moving_avg
        WHERE high_value_count >= 6
        RETURN a.account_id AS account_id, high_value_count, high_value_avg, max_moving_avg
        ORDER BY high_value_avg DESC
        """, # Query 2
        """
        
        """ # Query 3
    ]
    
    # Number of executions per query
    iterations = config.NUMBER_ITERATIONS
    results = []

    try:
        # Connect to Neo4j
        driver = GraphDatabase.driver(neo4j_config['uri'], auth=(neo4j_config['user'], neo4j_config['password']))
        
        print("Connected to the Neo4j database successfully.")
        
        for query in queries:
            times = []  # Store execution times for the current query
            
            # Run the query multiple times
            for _ in range(iterations):
                with driver.session() as session:
                    start_time = time.time()
                    session.run(query).consume()  # Ensure query execution completes
                    end_time = time.time()
                    times.append(end_time - start_time)
            
            # Calculate metrics
            avg_time = sum(times) / iterations
            std_dev = statistics.stdev(times)
            max_time = max(times)
            
            # Append results
            results.append((query, avg_time, std_dev, max_time, times))
    
    except Exception as e:
        print(f"Error occurred: {e}")
    
    finally:
        if driver:
            driver.close()
        print("Neo4j database connection closed.")

    # Display results in a table
    display_results(results)





if __name__ == "__main__":
    run_queries_and_analyze()
