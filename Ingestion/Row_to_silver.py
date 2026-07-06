from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime, timedelta
from delta.tables import DeltaTable
import dlt

today = datetime.now().strftime('%Y-%m-%d')

today_str = datetime.now().strftime('%Y%m%d')

mobile_regex=r"[^0-9+]"

payment_due=r"[^\d]"

Raw_volume_path="/Volumes/workspace/default/supplychain/Raw_data/"

silver_volume_path="/Volumes/workspace/default/supplychain/silver_data/"

processed_volume_path="/Volumes/workspace/default/supplychain/processed_raw_files/"

spark=SparkSession.builder\
        .appName("SupplyChain")\
        .getOrCreate()


Load="Incremental"
print(f"load={Load} and date={today}")

#log table
def log_file(file_name, processed_date, load_mode, status,Layer, raw_count=0, notes=""):
    log_df=spark.createDataFrame([{
        "file_name":file_name,
        "processed_date":processed_date,
        "load_mode":load_mode,
        "status":status,
        "layer":Layer,
        "raw_count":raw_count,
        "notes": notes
    }])

    DeltaTable.forName(spark, "supply_chain.control.processed_files")\
        .alias("t")\
        .merge(log_df.alias("s"), "t.file_name=s.file_name and t.layer=s.layer")\
        .whenMatchedUpdateAll()\
        .whenNotMatchedInsertAll()\
        .execute()

def move_to_processed(file_name, raw_path):
    dest_path=f"{processed_volume_path}{file_name}"

    try:
        dbutils.fs.mkdirs(processed_volume_path)
        dbutils.fs.cp(raw_path, dest_path)

        print(f"{file_name} copied at {dest_path}")
        try:
            dbutils.fs.ls(dest_path)
            copy_ok=True
        except Exception:
            copy_ok=False
        if copy_ok:
            dbutils.fs.rm(raw_path, recurse=False)
            print(f"{file_name} deleted from {raw_path}")
        else:
            print(f"Error while moving {file_name} from {raw_path} to {dest_path}")
        return dest_path
    except Exception as e:
        print(f"Error while moving {file_name} from {raw_path} to {dest_path}")
        raise Exception(f"Move failed for {file_name}: {str(e)}")

def get_unprocessed_file():
    all_files=[
        f for f in dbutils.fs.ls(Raw_volume_path)
        if f.name.endswith(".csv")
        ]
    print(f"total file to  process {len(all_files)}")
    sucess_name={
        raw["file_name"]
        for raw in spark.sql("""
            SELECT file_name FROM supply_chain.control.processed_files
            WHERE LAYER='raw' AND STATUS='SUCCESS' """
        ).collect()
    }
    print(f"total success file {len(sucess_name)}")

    failed_name={
        raw["file_name"]
        for raw in spark.sql("""
            SELECT file_name FROM supply_chain.control.processed_files
            WHERE LAYER='raw' AND STATUS='FAILED' """
        ).collect()
    }
    print(f"total failed files {len(failed_name)}")

    to_process=[
        f for f in all_files
        if f.name not in sucess_name
        or f.name in failed_name
    ]

    print(f"files queued for processing {len(to_process)}")
    return to_process

def write_silver(df, table_name, file_name):
    base=file_name.replace(".csv", "")
    parts=base.split("_")
    suffix=parts[-1] if parts[-1].isdigit() else today_str
    path=f"{silver_volume_path}{today_str}/silver_{table_name}_{suffix}/"

    df.write\
        .format("delta")\
        .mode("overwrite")\
        .option("overwriteschema", "true")\
        .save(path)

    print(f"silver-{path}, count={df.count()}")
    return path

#--------------CLEANING FUNCTIONS---------------------


def clean_carriers(df, file_name):

    cleaned = (
        df.dropna(how="all")\
        .drop_duplicates()\
        .select([
            when(trim(col(c))==" ", None).otherwise(col(c)).alias(c)
            if isinstance(df.schema[c].dataType, StringType)
            else col(c)
            for c in df.columns
        ])\
        .withColumn("is_active", when(col("is_active")==1, True).otherwise(False))\
        .withColumn("load_date", lit(today))
    )
    path = write_silver(cleaned, "carriers", file_name)
    return cleaned, path


def clean_warehouse(df, file_name):
    cleaned=(
        df.dropna(how="all")\
        .drop_duplicates()\
        .select([
            when(trim(col(c))==" ", None).otherwise(col(c)).alias(c)
            if isinstance(df.schema[c].dataType, StringType)
            else(col(c))
            for c in df.columns
        ])\
        .withColumn("phone", regexp_replace("phone", mobile_regex, ""))\
        .withColumn("is_active", when(col("is_active")==1, True).otherwise(False))\
        .withColumn("load_date", lit(today))
    )
    path = write_silver(cleaned, "warehouse", file_name)
    return cleaned, path


def clean_supplier(df, file_name):

    cleaned= (
        df.dropna(how="all")\
        .drop_duplicates()\
        .select([
            when(trim(col(c))==" ", None).otherwise(col(c)).alias(c)
            if isinstance(df.schema[c].dataType, StringType)
            else(col(c))
            for c in df.columns
        ])\
        .withColumn("payment_due", regexp_replace("payment_terms", payment_due, "").cast("int"))\
        .withColumn("phone", regexp_replace("phone", mobile_regex, ""))\
        .withColumn("load_date", lit(today))\
    )
    path = write_silver(cleaned, "supplier", file_name)
    return cleaned, path


def clean_inventory(df, file_name):

    cleaned=(
        df.dropna(how="all")\
        .drop_duplicates()\
        .select([
            when(trim(col(c))==" ", None).otherwise(col(c)).alias(c) if isinstance(df.schema[c].dataType, StringType)
            else col(c)
            for c in df.columns
        ])\
        .withColumn("calculated_quantity_available", col("quantity_on_hand")-col("quantity_reserved"))\
        .withColumn("final_quantity_available",
                when(col("quantity_available").isNotNull(), col("quantity_available"))
                .otherwise(col("quantity_on_hand") - col("quantity_reserved")))
        .withColumn("calculated_inventory_value", round(col("quantity_on_hand")*col("unit_cost"),2))\
        .withColumn("quantity_status",
                    when(col("calculated_quantity_available")<=0, "Out Of Stock")
                    .when(col("calculated_quantity_available")<=col("reorder_point"), "Low Quantity")
                    .otherwise("Healthy")
                    )\
        .withColumn("is_below_reorder", when(col("final_quantity_available")<=col("reorder_point"), lit(True))
                    .otherwise(lit(False)))\
        .withColumn("load_date", lit(today))
    )
    path = write_silver(cleaned, "inventory", file_name)
    return cleaned, path


def clean_shipment(df, file_name):
    cleaned=(
        df.dropna(how="all")
        .drop_duplicates()
        .select([
            when(trim(col(c))=="", None).otherwise(col(c)).alias(c)
            if isinstance(df.schema[c].dataType, StringType)
            else(col(c))
            for c in df.columns
        ])
        .withColumn("is_delayed", when(col("expected_delivery")==col("actual_delivery"), 'N')
        .when(col("actual_delivery")>col("expected_delivery"), 'Y').otherwise('Unknown'))
        .withColumn("delivery_status",
        when(col("quantity_delivered").isNull(), "No Delivery")
                    .when(col("quantity_ordered")==col("quantity_delivered"), "Fully Delivered")
        .otherwise("Partial Delivery"))
        .withColumn("unit_price_clean", col("unit_price").cast(DoubleType()))
        .withColumn("quantity_delivered_clean", col("quantity_delivered").cast(DoubleType()))
        .withColumn("effective_delivered_value", round(col("quantity_delivered_clean")*col("unit_price_clean"), 2)
        .cast("double"))\
        .withColumn("lost_in_transit", when(col("quantity_delivered").isNull(), None)
        .otherwise(col("quantity_shipped")-col("quantity_delivered")))
        .withColumn("load_date", lit(today))
    )
    path = write_silver(cleaned, "shipment", file_name)
    return cleaned, path


def clean_orders(df, file_name):

    cleaned=(
        df.dropna(how="all")
        .drop_duplicates()
        .select([
            when(trim(col(c))=="", None).otherwise(col(c)).alias(c)
            if isinstance(df.schema[c].dataType, StringType)
            else col(c)
            for c in df.columns
        ])
        .withColumn("load_date", lit(today))
        .withColumn("load_date", col("load_date").cast("date"))
    )
    path = write_silver(cleaned, "orders", file_name)
    return cleaned, path


FILE_ROUTING = {
    "carriers.csv":       clean_carriers,
    "warehouses.csv":     clean_warehouse,
    "suppliers.csv":      clean_supplier,
    "shipments":          clean_shipment,
    "orders":             clean_orders,
    "inventory_snapshot": clean_inventory,
}

def file_routing(file_name):
    if file_name in FILE_ROUTING:
        return FILE_ROUTING[file_name]
    for prefix, fn in FILE_ROUTING.items():
        if file_name.startswith(prefix+"_") and file_name.endswith(".csv"):
            return fn
    return None


#--------------Incremental Load log

def run_pipeline():
    print("Finding processing files...")
    files_to_process=get_unprocessed_file()

    if not files_to_process:
        print("No file to process")
        return
    
    processed_count = skipped_count = failed_count = 0

    print("Processing files...")

    for file in files_to_process:
        file_name=file.name
        file_path=file.path

        print(f"\n---{file_name}---")
        
        routing_fn=file_routing(file_name)

        if routing_fn is None:
            log_file(
                file_name, 
                status="SKIPPED",
                processed_date=today, 
                load_mode=Load,
                Layer="raw",
                notes="NO ROUTING MATCHED"
                )
            print(f"NO ROUTING MATCHED")
            skipped_count+=1
            continue
        
        try:
            df_raw = spark.read.csv(file_path, header=True, inferSchema=True)
            df_raw_count=df_raw.count()

            print(f"raw file count: {df_raw_count}")

            df_clean, silver_path = routing_fn(df_raw, file_name)
            clean_count = df_clean.count()

            print(f"clean file count: {clean_count}")

            processed_path=move_to_processed(file_name, file_path)
            
            log_file(
                file_name=file_name,
                raw_count=df_raw_count,
                processed_date=today, 
                load_mode=Load,
                status="SUCCESS",
                Layer="raw",
                notes=f"raw file count: {df_raw_count}, clean file count: {clean_count}"
            )

            processed_count+=1
        except Exception as e:
            print(f"Failed to process file: {file_name}, {str(e)[:200]}")
            print(e)
            log_file(
                file_name=file_name,
                processed_date=today, 
                load_mode=Load,
                Layer="raw",
                status="FAILED", 
                notes=str(e)[:300]
                )
            failed_count+=1
    #summary
    print(f"\n\nSummary:")
    print(f"processed: {processed_count}, skipped: {skipped_count}, failed: {failed_count}")
    print(f"processed: {processed_count+skipped_count+failed_count} total")

    if failed_count>0:
        raise Exception(f"Failed to process {failed_count} files")


run_pipeline()

@dlt.table
def dummy_table():
    return spark.range(1)
dummy_table()




