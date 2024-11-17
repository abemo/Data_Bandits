from neo4j import GraphDatabase
import time
import statistics
from prettytable import PrettyTable

def run_queries_and_analyze():
    # Connection details
    neo4j_config = {
        'uri': "bolt://localhost:7687",  # Change if using a different host or port
        'user': "neo4j",
        'password': "neo4j_password"
    }
    
    # Define three Cypher queries to execute
    queries = [
        "MATCH (n) RETURN n LIMIT 10",         # Query 1: Return nodes
        "MATCH (n) RETURN COUNT(n)",          # Query 2: Count nodes
        "MATCH (n) RETURN AVG(n.property)"    # Query 3: Average property value
    ]
    
    # Number of executions per query
    iterations = 1000
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


def display_results(results):
    # Create and configure a table
    table = PrettyTable()
    table.field_names = ["Query", "Average Time (s)", "Standard Deviation (s)", "Maximum Time (s)"]
    table.align["Query"] = "l"

    for query, avg_time, std_dev, max_time in results:
        if len(query) > 50:
            query = query[:50] + "..."
        table.add_row([query, f"{avg_time:.6f}", f"{std_dev:.6f}", f"{max_time:.6f}"])
    
    print("\nQuery Performance Metrics:")
    print(table)


if __name__ == "__main__":
    run_queries_and_analyze()
