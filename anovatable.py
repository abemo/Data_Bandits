import pandas as pd
import statisticalanalysis as sa

# Read your data
df = pd.read_csv("DBMS Experiment Data.csv")

# Calculate ANOVA table
anova_results = sa.calculate_anova_table(df)

# Display results
print(anova_results.to_string(index=False))