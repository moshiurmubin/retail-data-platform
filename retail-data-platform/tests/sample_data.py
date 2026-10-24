"""
Small Bronze-shaped samples that mimic the three real sources, including the
messy parts: names as "LAST, FIRST", postcodes as 94043.0, epoch timestamps,
numbers that arrive as strings in one document and as numbers in the next,
a duplicate customer, a customer with no id, and an order from a customer who
is not in the CRM.
"""
import json
from datetime import datetime
from pathlib import Path

from common_utils.config import load_config

TS = datetime(2026, 10, 24, 5, 0, 0)
CONFIG_ROOT = Path(__file__).resolve().parents[1] / "src"


def cfg(path):
    return load_config(f"{CONFIG_ROOT}/{path}", catalog="test")


def bronze_customers(spark):
    rows = [
        # customer_id, customer_name, tax_id, tax_code, state, city, postcode, street, number, unit,
        # region, district, lon, lat, ship_to_address, valid_from, valid_to, units_purchased, loyalty_segment
        (16738781, "GALLARDO, JOHN D", 123.0, "A", "ca", "mountain view", 94043.0, "Main St", "1", None,
         "West", "D1", -122.08, 37.38, "addr", 1532131300.0, None, 10.0, 1),
        (16738781, "GALLARDO, JOHN D", 123.0, "A", "ca", "mountain view", 94043.0, "Main St", "1", None,
         "West", "D1", -122.08, 37.38, "addr", 1532131300.0, None, 10.0, 1),   # exact duplicate
        (20000002, "SMITH, ANNA", None, None, "ny", "new york", 10001.0, "5th Ave", "9", "2",
         "East", "D2", -73.99, 40.75, "addr2", 1540000000.0, None, 3.0, 2),
        (20000003, "MADONNA", None, None, "tx", "austin", 73301.0, "Lake", "3", None,
         "South", "D3", -97.7, 30.2, "addr3", 1550000000.0, None, 1.0, None),   # no comma in name
        (None, "NOBODY, NO ID", None, None, "tx", "austin", 73301.0, "x", "1", None,
         "South", "D3", 0.0, 0.0, "a", None, None, 0.0, 0),                      # quarantine
    ]
    schema = ("customer_id int, customer_name string, tax_id double, tax_code string, state string, "
              "city string, postcode double, street string, number string, unit string, region string, "
              "district string, lon double, lat double, ship_to_address string, valid_from double, "
              "valid_to double, units_purchased double, loyalty_segment int")
    return _with_audit(spark.createDataFrame(rows, schema))


ORDER_1 = {
    "_id": {"$oid": "64f1"}, "customer_id": 16738781, "customer_name": "GALLARDO, JOHN D",
    "number_of_line_items": 2, "order_datetime": 1654647864, "order_number": 317568043,
    "ordered_products": [
        {"curr": "USD", "id": "P101", "name": "Laptop", "price": 1200, "promotion_info": None, "qty": 2, "unit": "pcs"},
        {"curr": "USD", "id": "P102", "name": "Mouse", "price": "30", "promotion_info": None, "qty": "1", "unit": "pcs"},
    ],
    "promo_info": [{"promo_disc": 0.1, "promo_id": "PR1", "promo_item": "P101", "promo_qty": 1}],
    "clicked_items": [["P101", "70"], ["P103", 98]],
}
ORDER_2 = {   # numbers as strings, epoch in scientific notation, customer not in CRM
    "_id": {"$oid": "64f2"}, "customer_id": "99999999", "customer_name": "GHOST, CASPER",
    "number_of_line_items": "1", "order_datetime": "1.564627663E9", "order_number": "317568044",
    "ordered_products": [{"curr": "USD", "id": "P102", "name": "Mouse", "price": 30.0, "qty": 3, "unit": "pcs"}],
    "promo_info": [],
    "clicked_items": [["P104"]],   # short inner array: score missing
}
ORDER_BAD = {"order_number": None, "customer_id": 1, "ordered_products": [], "promo_info": None,
             "clicked_items": None, "order_datetime": "x"}


def bronze_sales_orders(spark):
    docs = [ORDER_1, ORDER_2, ORDER_1, ORDER_BAD]   # ORDER_1 twice: duplicate document
    df = spark.createDataFrame([(json.dumps(d),) for d in docs], "json_data string")
    return _with_audit(df)


def bronze_s3_sales(spark):
    def product(pid, name, price, qty):
        return json.dumps({"curr": "USD", "id": pid, "name": name, "price": price, "qty": qty, "unit": "pcs"})

    rows = [
        (16738781, "GALLARDO, JOHN D", "Laptop", "2022-06-08", "Computers", product("P101", "Laptop", 1200, 1), 1200.0),
        (20000002, "SMITH, ANNA", "Cable", "2022-06-09", "Accessories", product("P200", "Cable", "5.5", "2"), 11.0),
        (20000002, "SMITH, ANNA", "Cable", "2022-06-09", "Accessories", product("P200", "Cable", "5.5", "2"), 11.0),
        (None, "X", "Cable", "2022-06-09", "Accessories", product("P200", "Cable", 5.5, 1), 5.5),
    ]
    schema = ("customer_id long, customer_name string, product_name string, order_date string, "
              "product_category string, product string, total_price double")
    return _with_audit(spark.createDataFrame(rows, schema))


def _with_audit(df):
    import pyspark.sql.functions as F

    return (
        df.withColumn("last_update_ts", F.lit(TS))
        .withColumn("file_path", F.lit("/Volumes/test/bronze/raw_data/x/part-0"))
        .withColumn("load_date", F.to_date(F.lit("2026-10-24")))
        .withColumn("source_system", F.lit("test"))
    )
