import pandas as pd
import numpy as np

# Sample DataFrame
data = pd.read_csv(r'D:\CivicDataLab_IDS-DRR\IDS-DRR_Github\Deployment\IDS-DRR-Assam-Risk-Model\RiskScoreModel\data\risk_score.csv')

df = pd.DataFrame(data)

# Melt the DataFrame
df_melted = df.melt(id_vars=['district', 'timeperiod'], var_name='indicator', value_name='value')

# Pivot the DataFrame
df_pivot = df_melted.pivot(index=['timeperiod', 'indicator'], columns='district', values='value').reset_index()

# Reorder columns to match the desired output format
df_pivot = df_pivot[['SOUTH SALMARA MANCACHAR', 'CACHAR', 'BARPETA', 'BONGAIGAON', 'timeperiod', 'indicator']]

# Print result
print(df_pivot)
