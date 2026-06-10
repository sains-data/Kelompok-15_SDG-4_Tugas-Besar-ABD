import pandas as pd

df = pd.read_excel('/opt/airflow/jobs/data.xlsx', sheet_name='Jawa Barat', header=[6, 7], nrows=5)

# Fix Unnamed columns in level 0
new_cols = []
for c0, c1 in df.columns:
    if 'Unnamed' in c0:
        new_cols.append(c1.strip().replace('\n', ''))
    else:
        new_cols.append(f"{c0.strip()}|{c1.strip()}")
df.columns = new_cols

id_vars = [c for c in df.columns if '|' not in c]
value_vars = [c for c in df.columns if '|' in c]

df_melt = df.melt(id_vars=id_vars, value_vars=value_vars, var_name='indikator_metrik', value_name='nilai')

# split indikator_metrik
df_melt[['indikator', 'metrik']] = df_melt['indikator_metrik'].str.split('|', expand=True, n=1)
df_melt.drop('indikator_metrik', axis=1, inplace=True)

# pivot metrik
df_pivot = df_melt.pivot_table(index=id_vars + ['indikator'], columns='metrik', values='nilai', aggfunc='first').reset_index()

# clean column names
df_pivot.columns.name = None
df_pivot.columns = [c.lower().replace(' ', '_').replace('/', '_') for c in df_pivot.columns]

# extract kode_indikator
df_pivot['kode_indikator'] = df_pivot['indikator'].apply(lambda x: x.split(' ')[0] if isinstance(x, str) else x)

print(df_pivot.head())
print(df_pivot.columns)
