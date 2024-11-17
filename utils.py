from prettytable import PrettyTable

def display_results(results):
    # Create and configure a table
    table = PrettyTable()
    table.field_names = ["Query", "Average Time (s)", "Standard Deviation (s)", "Maximum Time (s)", "Times"]
    table.align["Query"] = "l"

    for query, avg_time, std_dev, max_time, times in results:
        if len(query) > 50:
            query = query[:50] + "..."
        times = [f"{time:.2f}" for time in times]
        table.add_row([query, f"{avg_time:.6f}", f"{std_dev:.6f}", f"{max_time:.6f}", f"{times}"])
    
    print("\nQuery Performance Metrics:")
    print(table)