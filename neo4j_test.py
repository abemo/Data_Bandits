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
        """
        MATCH (a:Account)
        OPTIONAL MATCH (a)-[:HAS_TRANSACTION]->(t:Transaction)
        WHERE t.date >= date() - duration('P1Y') AND t.date <= date()
        WITH a, collect(t) AS transactions
        WHERE size(transactions) > 100
        MATCH (a)<-[:BELONGS_TO]-(d:Disposition)-[:OWNED_BY]->(c:Client)
        WITH a, transactions, c,
            size(collect(DISTINCT t.trans_id)) AS transaction_count,
            reduce(sum = 0, t IN transactions | sum + t.amount) AS total_amount,
            reduce(sum = 0.0, t IN transactions | sum + t.amount) / size(transactions) AS avg_transaction_amount,
            reduce(max = 0, t IN transactions | CASE WHEN t.amount > max THEN t.amount ELSE max END) AS max_transaction_amount
        ORDER BY total_amount DESC
        LIMIT 100
        RETURN
            a.account_id,
            transaction_count,
            total_amount,
            avg_transaction_amount,
            max_transaction_amount;
        """,         # Query 1
        """
        MATCH (c:Client)-[:HAS_DISPOSITION]->(d:Disposition {type: 'OWNER'})-[:BELONGS_TO]->(a:Account)-[:HAS_TRANSACTION]->(t:Transaction)
        WHERE t.date >= date() - duration('P6M')
        WITH a, t
        ORDER BY a.account_id, t.date
        // Now, let's calculate the moving average and rank
        WITH a, collect(t) AS transactions
        UNWIND range(0, size(transactions)-1) AS i
        WITH a, transactions[i] AS t,
            transactions[i].amount AS amount,
            [j IN range(i-2, i) WHERE j >= 0 | transactions[j].amount] AS prev_amounts
        WITH a, t, amount,
            CASE WHEN size(prev_amounts) > 0 THEN (sum(prev_amounts) + amount) / (size(prev_amounts) + 1) ELSE amount END AS moving_avg,
            size(collect(t)) OVER (a) AS total_trans,
            rank(t.amount) OVER (a ORDER BY t.amount DESC) AS amount_rank
        // Filter for top 10 transactions by amount and where amount > 1.5 * moving_avg
        WHERE amount_rank <= 10 AND amount > moving_avg * 1.5
        // Group by account and calculate required metrics
        WITH a.account_id AS account_id,
            count(*) AS high_value_transactions,
            avg(amount) AS avg_high_value_amount,
            max(moving_avg) AS max_moving_avg
        WHERE high_value_transactions > 5
        // Order and return results
        RETURN
            account_id,
            high_value_transactions,
            avg_high_value_amount,
            max_moving_avg
        ORDER BY avg_high_value_amount DESC;
        """,          # Query 2
        """
        CREATE INDEX IF NOT EXISTS FOR (c:Client) ON (c.client_id);
        CREATE INDEX IF NOT EXISTS FOR (t:Transaction) ON (t.date);
        CREATE INDEX IF NOT EXISTS FOR (t:Transaction) ON (t.amount);
        """    # Query 3
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
