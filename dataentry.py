import pandas as pd
import tkinter as tk
from tkinter import ttk, messagebox
import csv
import os

class DatabaseQueryEntry:
    def __init__(self, root):
        self.root = root
        self.root.title("DBMS Query Time Entry")
        
        # Data storage
        self.databases = ["Neo4j", "Timescale", "PostgreSQL"]
        self.queries = ["Query 1", "Query 2", "Query 3"]
        self.num_replications = 5
        
        # Load existing data if available
        self.load_existing_data()
        
        # Track progress
        self.current_db_index = 0
        self.current_query_index = 0
        self.current_replication = 1
        
        self.setup_gui()
        
    def load_existing_data(self):
        self.filename = 'DBMS Experiment Data.csv'
        if os.path.exists(self.filename):
            self.df = pd.read_csv(self.filename)
            # Fill missing entries with empty string for checking completion
            self.df['Response_Time'] = self.df['Response_Time'].fillna('')
        else:
            # Create empty DataFrame with all combinations
            computers = ["Danielle", "Peter", "Harsh"]
            combinations = []
            for computer in computers:
                for db in self.databases:
                    for query in self.queries:
                        for rep in range(1, self.num_replications + 1):
                            combinations.append({
                                'Computer': computer,
                                'Database': db,
                                'Query': query,
                                'Replication': rep,
                                'Response_Time': ''
                            })
            self.df = pd.DataFrame(combinations)
            self.df.to_csv(self.filename, index=False)
        
    def setup_gui(self):
        # Computer selection
        tk.Label(self.root, text="Select your name:").pack(pady=5)
        self.computer_var = tk.StringVar()
        self.computer_combo = ttk.Combobox(self.root, 
                                         textvariable=self.computer_var,
                                         values=["Danielle", "Peter", "Harsh"],
                                         state="readonly")
        self.computer_combo.pack(pady=5)
        self.computer_combo.bind('<<ComboboxSelected>>', self.on_computer_select)
        
        # Database selection
        tk.Label(self.root, text="Select Database:").pack(pady=5)
        self.db_var = tk.StringVar()
        self.db_combo = ttk.Combobox(self.root, 
                                    textvariable=self.db_var,
                                    values=self.databases,
                                    state="readonly")
        self.db_combo.pack(pady=5)
        self.db_combo.bind('<<ComboboxSelected>>', self.on_database_select)
        
        # Current entry frame
        entry_frame = tk.LabelFrame(self.root, text="Current Entry", padx=10, pady=5)
        entry_frame.pack(pady=10, padx=10, fill="x")
        
        # Query and replication display
        tk.Label(entry_frame, text="Query:").pack()
        self.query_label = tk.Label(entry_frame, text="", font=('Arial', 12, 'bold'))
        self.query_label.pack()
        
        tk.Label(entry_frame, text="Replication:").pack()
        self.rep_label = tk.Label(entry_frame, text="", font=('Arial', 12, 'bold'))
        self.rep_label.pack()
        
        # Response time entry
        tk.Label(entry_frame, text="Response Time (seconds):").pack(pady=5)
        self.time_entry = tk.Entry(entry_frame)
        self.time_entry.pack(pady=5)
        
        # Submit button
        self.submit_btn = tk.Button(self.root, text="Submit Time", command=self.submit_time)
        self.submit_btn.pack(pady=10)
        
        # Progress display
        self.progress_label = tk.Label(self.root, text="")
        self.progress_label.pack(pady=5)
        
        # Error message display
        self.error_label = tk.Label(self.root, text="", fg="red")
        self.error_label.pack(pady=5)
        
    def on_computer_select(self, event=None):
        self.update_progress_display()
        
    def on_database_select(self, event=None):
        self.find_next_entry()
        
    def find_next_entry(self):
        if not self.computer_var.get() or not self.db_var.get():
            return
            
        # Filter for current computer and database
        mask = (self.df['Computer'] == self.computer_var.get()) & \
               (self.df['Database'] == self.db_var.get()) & \
               (self.df['Response_Time'] == '')
        
        if mask.any():
            next_entry = self.df[mask].iloc[0]
            self.query_label.config(text=next_entry['Query'])
            self.rep_label.config(text=f"Replication {next_entry['Replication']}")
            self.update_progress_display()
        else:
            self.query_label.config(text="No more entries needed")
            self.rep_label.config(text="")
            self.time_entry.config(state="disabled")
            self.submit_btn.config(state="disabled")
            
    def submit_time(self):
        try:
            response_time = float(self.time_entry.get())
            if response_time <= 0:
                raise ValueError
        except ValueError:
            self.error_label.config(text="Please enter a valid positive number")
            return
            
        # Update DataFrame
        mask = (self.df['Computer'] == self.computer_var.get()) & \
               (self.df['Database'] == self.db_var.get()) & \
               (self.df['Query'] == self.query_label.cget("text")) & \
               (self.df['Replication'] == int(self.rep_label.cget("text").split()[-1]))
        
        self.df.loc[mask, 'Response_Time'] = response_time
        
        # Save to CSV
        self.df.to_csv(self.filename, index=False)
        
        # Clear entry and find next
        self.time_entry.delete(0, tk.END)
        self.error_label.config(text="")
        self.find_next_entry()
        
    def update_progress_display(self):
        if not self.computer_var.get():
            return
            
        total = len(self.df[self.df['Computer'] == self.computer_var.get()])
        completed = len(self.df[(self.df['Computer'] == self.computer_var.get()) & 
                              (self.df['Response_Time'] != '')])
        
        progress_pct = (completed / total) * 100
        self.progress_label.config(
            text=f"Progress: {completed}/{total} entries ({progress_pct:.1f}%)")

def main():
    root = tk.Tk()
    app = DatabaseQueryEntry(root)
    root.mainloop()

if __name__ == "__main__":
    main()