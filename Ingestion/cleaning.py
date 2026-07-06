from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime, timedelta

today = datetime.now().strftime('%Y-%m-%d')

today_str = datetime.now().strftime('%Y%m%d')

mobile_regex=r"[^0-9+]"

payment_due=r"[^\d]"

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

