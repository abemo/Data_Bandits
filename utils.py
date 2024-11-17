from prettytable import PrettyTable

def display_results(results):
    # Create and configure a table
    table = PrettyTable()
    # table.field_names = ["Query", "Average Time (s)", "Standard Deviation (s)", "Maximum Time (s)", "Times"]
    table.field_names = ["Query", "Time 1", "Time 2", "Time 3", "Time 4", "Time 5"]
    table.align["Query"] = "l"

    for query, avg_time, std_dev, max_time, times in results:
        if len(query) > 50:
            query = query[:50] + "..."
        times = [f"{time:.2f}" for time in times]
        # table.add_row([query, f"{avg_time:.6f}", f"{std_dev:.6f}", f"{max_time:.6f}"])
        table.add_row([query, times[0], times[1], times[2], times[3], times[4],] )
    
    print("\nQuery Performance Metrics:")
    print(table)