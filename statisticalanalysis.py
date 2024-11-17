import pandas as pd
import numpy as np
from scipy import stats

def calculate_anova_table(df):
    """
    Calculate complete ANOVA table for three-way design with replications
    
    Parameters:
    df: DataFrame with columns Database, Query, Computer, Response_Time, Replication
    
    Returns:
    DataFrame: ANOVA table with SS, df, MS, F-values, and p-values
    """
    # Calculate Sum of Squares components
    n_databases = len(df['Database'].unique())
    n_queries = len(df['Query'].unique())
    n_computers = len(df['Computer'].unique())
    n_replications = len(df['Replication'].unique())
    n_total = len(df)
    
    # Overall Mean
    overall_mean = df['Response_Time'].mean()
    
    # Total Sum of Squares
    SS_Total = np.sum((df['Response_Time'] - overall_mean) ** 2)
    
    # Main Effects
    db_means = df.groupby('Database')['Response_Time'].mean()
    SS_A = n_queries * n_computers * n_replications * np.sum((db_means - overall_mean) ** 2)
    
    query_means = df.groupby('Query')['Response_Time'].mean()
    SS_B = n_databases * n_computers * n_replications * np.sum((query_means - overall_mean) ** 2)
    
    computer_means = df.groupby('Computer')['Response_Time'].mean()
    SS_C = n_databases * n_queries * n_replications * np.sum((computer_means - overall_mean) ** 2)
    
    # Interaction Effects
    db_query_means = df.groupby(['Database', 'Query'])['Response_Time'].mean()
    SS_AB = n_computers * n_replications * np.sum(
        (db_query_means - 
         db_query_means.index.get_level_values('Database').map(db_means) - 
         db_query_means.index.get_level_values('Query').map(query_means) + 
         overall_mean) ** 2
    )
    
    db_comp_means = df.groupby(['Database', 'Computer'])['Response_Time'].mean()
    SS_AC = n_queries * n_replications * np.sum(
        (db_comp_means - 
         db_comp_means.index.get_level_values('Database').map(db_means) - 
         db_comp_means.index.get_level_values('Computer').map(computer_means) + 
         overall_mean) ** 2
    )
    
    query_comp_means = df.groupby(['Query', 'Computer'])['Response_Time'].mean()
    SS_BC = n_databases * n_replications * np.sum(
        (query_comp_means - 
         query_comp_means.index.get_level_values('Query').map(query_means) - 
         query_comp_means.index.get_level_values('Computer').map(computer_means) + 
         overall_mean) ** 2
    )
    
    # Three-way interaction
    db_query_comp_means = df.groupby(['Database', 'Query', 'Computer'])['Response_Time'].mean()
    SS_ABC = n_replications * np.sum(
        (db_query_comp_means - 
         db_query_comp_means.index.get_level_values('Database').map(db_means) -
         db_query_comp_means.index.get_level_values('Query').map(query_means) -
         db_query_comp_means.index.get_level_values('Computer').map(computer_means) +
         2 * overall_mean) ** 2
    )
    
    # Error
    SS_E = SS_Total - (SS_A + SS_B + SS_C + SS_AB + SS_AC + SS_BC + SS_ABC)
    
    # Degrees of Freedom
    df_A = n_databases - 1
    df_B = n_queries - 1
    df_C = n_computers - 1
    df_AB = df_A * df_B
    df_AC = df_A * df_C
    df_BC = df_B * df_C
    df_ABC = df_A * df_B * df_C
    df_E = n_total - (n_databases * n_queries * n_computers)
    df_Total = n_total - 1
    
    # Mean Squares
    MS_A = SS_A / df_A
    MS_B = SS_B / df_B
    MS_C = SS_C / df_C
    MS_AB = SS_AB / df_AB
    MS_AC = SS_AC / df_AC
    MS_BC = SS_BC / df_BC
    MS_ABC = SS_ABC / df_ABC
    MS_E = SS_E / df_E
    
    # F Values
    F_A = MS_A / MS_E
    F_B = MS_B / MS_E
    F_C = MS_C / MS_E
    F_AB = MS_AB / MS_E
    F_AC = MS_AC / MS_E
    F_BC = MS_BC / MS_E
    F_ABC = MS_ABC / MS_E
    
    # P Values
    p_A = 1 - stats.f.cdf(F_A, df_A, df_E)
    p_B = 1 - stats.f.cdf(F_B, df_B, df_E)
    p_C = 1 - stats.f.cdf(F_C, df_C, df_E)
    p_AB = 1 - stats.f.cdf(F_AB, df_AB, df_E)
    p_AC = 1 - stats.f.cdf(F_AC, df_AC, df_E)
    p_BC = 1 - stats.f.cdf(F_BC, df_BC, df_E)
    p_ABC = 1 - stats.f.cdf(F_ABC, df_ABC, df_E)
    
    # Create ANOVA table
    anova_table = pd.DataFrame({
        'Source': ['Database (A)', 'Query (B)', 'Computer (C)', 
                  'A × B', 'A × C', 'B × C', 'A × B × C', 
                  'Error', 'Total'],
        'SS': [SS_A, SS_B, SS_C, SS_AB, SS_AC, SS_BC, SS_ABC, 
               SS_E, SS_Total],
        'df': [df_A, df_B, df_C, df_AB, df_AC, df_BC, df_ABC, 
               df_E, df_Total],
        'MS': [MS_A, MS_B, MS_C, MS_AB, MS_AC, MS_BC, MS_ABC, 
               MS_E, np.nan],
        'F': [F_A, F_B, F_C, F_AB, F_AC, F_BC, F_ABC, 
              np.nan, np.nan],
        'p-value': [p_A, p_B, p_C, p_AB, p_AC, p_BC, p_ABC, 
                    np.nan, np.nan]
    })
    
    # Format the table
    anova_table['SS'] = anova_table['SS'].round(3)
    anova_table['MS'] = anova_table['MS'].round(3)
    anova_table['F'] = anova_table['F'].round(3)
    anova_table['p-value'] = anova_table['p-value'].round(4)
    
    return anova_table

# Example usage:
# anova_results = calculate_anova_table(df)
# print(anova_results.to_string(index=False))