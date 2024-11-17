from prettytable import PrettyTable

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