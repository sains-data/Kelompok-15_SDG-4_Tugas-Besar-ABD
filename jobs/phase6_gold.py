import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when

def get_spark_session():
    spark = SparkSession.builder \
        .appName("Silver_to_Gold_Pipeline") \
        .getOrCreate()
        
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.endpoint", "http://minio:9000")
    hadoop_conf.set("fs.s3a.access.key", "admin")
    hadoop_conf.set("fs.s3a.secret.key", "password123")
    hadoop_conf.set("fs.s3a.path.style.access", "true")
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    
    return spark

def write_to_postgres(df, table_name):
    # Setup PostgreSQL properties
    # using 'postgres' hostname because it's in the same docker network
    jdbc_url = "jdbc:postgresql://postgres:5432/airflow"
    connection_properties = {
        "user": "airflow",
        "password": "airflow",
        "driver": "org.postgresql.Driver"
    }
    print(f"Menyimpan ke PostgreSQL tabel {table_name}...")
    df.write.jdbc(url=jdbc_url, table=table_name, mode="overwrite", properties=connection_properties)

def process_gold():
    spark = get_spark_session()
    print("Memulai proses Analytics: Silver -> Gold")
    
    silver_path = "s3a://silver/fact_capaian_provinsi"
    
    try:
        df_silver = spark.read.parquet(silver_path)
        print(f"Data Silver dibaca: {df_silver.count()} baris.")
    except Exception as e:
        print("Gagal membaca Silver layer. Pastikan Phase 5 sudah berjalan sukses.")
        sys.exit(1)
        
    # Analisis 1: Distribusi Label per Provinsi dan Jenjang
    print("Membuat Agregasi 1: gold_distribusi_label")
    gold_distribusi = df_silver.groupBy("provinsi", "jenis_satuan_pendidikan", "label_capaian") \
        .agg(count("*").alias("jumlah_indikator"))
        
    gold_distribusi.write.mode("overwrite").parquet("s3a://gold/distribusi_label_per_provinsi")
    write_to_postgres(gold_distribusi, "gold_distribusi_label")
    
    # Analisis 2: Ketimpangan Wilayah (Label Kurang/Rendah)
    print("Membuat Agregasi 2: gold_ketimpangan_wilayah")
    gold_ketimpangan = df_silver.filter(col("label_capaian").isin(["kurang", "rendah"])) \
        .groupBy("provinsi", "kabupaten_kota", "kode_indikator", "indikator") \
        .agg(count("*").alias("jumlah_isu_kritis"))
        
    gold_ketimpangan.write.mode("overwrite").parquet("s3a://gold/ketimpangan_indikator_wilayah")
    write_to_postgres(gold_ketimpangan, "gold_ketimpangan_wilayah")
    
    # Analisis 3: Top 10 Wilayah Terbaik & Terendah berdasarkan persentase label Baik
    print("Membuat Agregasi 3: gold_performa_wilayah")
    gold_performa = df_silver.groupBy("provinsi", "kabupaten_kota") \
        .agg(
            count("*").alias("total_indikator"),
            count(when(col("label_capaian") == "baik", 1)).alias("indikator_baik"),
            count(when(col("label_capaian").isin(["kurang", "rendah"]), 1)).alias("indikator_buruk")
        )
    # Hitung persentase
    gold_performa = gold_performa.withColumn("persentase_baik", (col("indikator_baik") / col("total_indikator")) * 100)
    
    gold_performa.write.mode("overwrite").parquet("s3a://gold/performa_wilayah")
    write_to_postgres(gold_performa, "gold_performa_wilayah")

    print("Proses Silver -> Gold SELESAI. Dashboard Superset kini bisa diupdate.")
    spark.stop()

if __name__ == "__main__":
    process_gold()
