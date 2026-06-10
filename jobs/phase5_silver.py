import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, lower, when

def get_spark_session():
    spark = SparkSession.builder \
        .appName("Bronze_to_Silver_Pipeline") \
        .getOrCreate()
        
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.endpoint", "http://minio:9000")
    hadoop_conf.set("fs.s3a.access.key", "admin")
    hadoop_conf.set("fs.s3a.secret.key", "password123")
    hadoop_conf.set("fs.s3a.path.style.access", "true")
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    
    return spark

def process_silver():
    spark = get_spark_session()
    
    print("Memulai proses ETL: Bronze -> Silver (Data Cleaning)")
    
    bronze_path = "s3a://bronze/combined_data.csv"
    
    try:
        # Membaca data mentah
        df = spark.read.csv(bronze_path, header=True, inferSchema=True)
        print(f"Data Bronze berhasil dibaca: {df.count()} baris.")
    except Exception as e:
        print("Gagal membaca dari Bronze layer. Pastikan Phase 2 (Upload) sudah berjalan.")
        sys.exit(1)
        
    # Tahap 1: Menghapus Duplicate
    print("Menghapus baris duplikat...")
    df_clean = df.dropDuplicates()
    
    # Tahap 2: Menangani Missing Value & Standardisasi Teks
    print("Membersihkan nama kolom, standarisasi huruf, dan missing value...")
    
    # Standarisasi kolom penting (jika ada)
    columns_to_clean = ["provinsi", "kabupaten_kota", "nama_satuan_pendidikan", "label_capaian"]
    
    for c in columns_to_clean:
        if c in df_clean.columns:
            # lowercase dan trim
            df_clean = df_clean.withColumn(c, lower(trim(col(c))))
            # isi missing value string
            df_clean = df_clean.withColumn(c, when(col(c).isNull() | (col(c) == ""), "tidak diketahui").otherwise(col(c)))

    # Tahap 3: Validasi kolom wajib (Contoh: provinsi harus ada)
    if "provinsi" not in df_clean.columns:
        print("ERROR: Kolom 'provinsi' tidak ditemukan di dataset!")
        sys.exit(1)
        
    # Filter data kosong (jika ada baris yang benar-benar tidak ada nilainya di kolom krusial)
    df_clean = df_clean.filter(col("provinsi").isNotNull() & (col("provinsi") != "tidak diketahui"))
    
    # Simpan ke Silver Layer
    silver_path = "s3a://silver/fact_capaian_provinsi"
    
    print("Menyimpan data yang sudah bersih ke Silver Layer (Parquet format)...")
    df_clean.write.mode("overwrite") \
            .partitionBy("provinsi") \
            .parquet(silver_path)
            
    print("Proses Bronze -> Silver SELESAI.")
    spark.stop()

if __name__ == "__main__":
    process_silver()
