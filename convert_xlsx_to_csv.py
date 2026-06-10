import pandas as pd
import boto3
import os

print("Membaca XLSX file dengan struktur multi-level header...")
xl = pd.ExcelFile("/opt/airflow/jobs/data.xlsx")
sheet_names = xl.sheet_names

all_data = []
for sheet in sheet_names:
    if sheet.lower() in ['metadata', 'referensi', 'cover', 'nasional']:
        continue
        
    print(f"Membaca & Transforming sheet: {sheet}")
    try:
        df = pd.read_excel(xl, sheet_name=sheet, header=[6, 7])
        
        # Flatten headers
        new_cols = []
        for c0, c1 in df.columns:
            if 'Unnamed' in c0:
                new_cols.append(c1.strip().replace('\n', ''))
            else:
                new_cols.append(f"{c0.strip()}|{c1.strip()}")
        df.columns = new_cols

        # Identify columns
        id_vars = [c for c in df.columns if '|' not in c]
        value_vars = [c for c in df.columns if '|' in c]

        # Melt
        df_melt = df.melt(id_vars=id_vars, value_vars=value_vars, var_name='indikator_metrik', value_name='nilai')

        # Split indikator and metrik
        df_melt[['indikator', 'metrik']] = df_melt['indikator_metrik'].str.split('|', expand=True, n=1)
        df_melt.drop('indikator_metrik', axis=1, inplace=True)

        # Pivot
        df_pivot = df_melt.pivot_table(index=id_vars + ['indikator'], columns='metrik', values='nilai', aggfunc='first').reset_index()

        # Clean names
        df_pivot.columns.name = None
        df_pivot.columns = [str(c).lower().replace(' ', '_').replace('/', '_').replace('-', '_').replace('2025', '') for c in df_pivot.columns]

        # Ensure trailing underscores removed
        df_pivot.columns = [c.strip('_') for c in df_pivot.columns]
        
        # Add province if missing
        if 'provinsi' not in df_pivot.columns:
            df_pivot['provinsi'] = sheet
            
        # extract kode_indikator
        df_pivot['kode_indikator'] = df_pivot['indikator'].apply(lambda x: x.split(' ')[0] if isinstance(x, str) else x)

        all_data.append(df_pivot)
    except Exception as e:
        print(f"Error on sheet {sheet}: {e}")

if all_data:
    final_df = pd.concat(all_data, ignore_index=True)
    
    # Rename commonly queried columns to standard names used by PySpark Phase 5 & 6
    final_df.rename(columns={
        'label_capaian': 'label_capaian',
        'perubahan_nilai_capaian_dari_tahun_lalu': 'perubahan_capaian'
    }, inplace=True)
    
    csv_path = "/opt/airflow/jobs/combined_data.csv"
    final_df.to_csv(csv_path, index=False)
    print(f"Berhasil menggabungkan sheet menjadi format LONG dengan total {len(final_df)} baris.")
    
    # Upload to MinIO Bronze
    print("Mengunggah ke MinIO bucket 'bronze'...")
    s3 = boto3.client('s3',
                      endpoint_url='http://minio:9000',
                      aws_access_key_id='admin',
                      aws_secret_access_key='password123')
    
    try:
        s3.create_bucket(Bucket='bronze')
    except:
        pass
        
    s3.upload_file(csv_path, 'bronze', 'combined_data.csv')
    print("Selesai mengunggah ke s3a://bronze/combined_data.csv")
else:
    print("Tidak ada data yang diproses.")
