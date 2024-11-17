from neo4j import GraphDatabase
import time
import statistics
from utils import display_results
import config

def run_queries_and_analyze():
    # Connection details
    neo4j_config = {
        'uri': config.NEO4J_URI,
        'user': config.NEO4J_USER,
        'password': config.NEO4J_PASSWORD
    }
    
    # Define three Cypher queries to execute
    queries = [
        "MATCH (n) RETURN n LIMIT 10",         # Query 1: Return nodes
        "MATCH (n) RETURN COUNT(n)",          # Query 2: Count nodes
        "MATCH (n) RETURN AVG(n.property)"    # Query 3: Average property value
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
            results.append((query, avg_time, std_dev, max_time))
    
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
