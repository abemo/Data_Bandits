import psycopg2
import time
import statistics
from prettytable import PrettyTable

def run_queries_and_analyze():
    # Connection details
    db_config = {
        'dbname': 'financial',
        'user': 'postgres',
        'password': 'postgres',
        'host': 'localhost',
        'port': '5432'
    }
    
    # Define three queries to execute
    queries = [
        "SELECT * FROM card LIMIT 10",  # Query 1
        "SELECT COUNT(*) FROM trans",    # Query 2
        "SELECT AVG(amount) FROM loan"  # Query 3
    ]
    
    # Number of executions per query
    iterations = 1000
    results = []

    try:
        # Connect to PostgreSQL database
        connection = psycopg2.connect(**db_config)
        cursor = connection.cursor()
        
        print("Connected to the database successfully.")
        
        for query in queries:
            times = []  # Store execution times for the current query
            
            for _ in range(iterations):
                start_time = time.time()
                cursor.execute(query)
                cursor.fetchall()  # Ensure the query completes execution
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
        if cursor:
            cursor.close()
        if connection:
            connection.close()
        print("Database connection closed.")

    # Display results in a table
    display_results(results)


def display_results(results):
    # Create and configure a table
    table = PrettyTable()
    table.field_names = ["Query", "Average Time (s)", "Standard Deviation (s)", "Maximum Time (s)"]
    table.align["Query"] = "l"

    for query, avg_time, std_dev, max_time in results:
        table.add_row([query, f"{avg_time:.6f}", f"{std_dev:.6f}", f"{max_time:.6f}"])
    
    print("\nQuery Performance Metrics:")
    print(table)


if __name__ == "__main__":
    run_queries_and_analyze()
