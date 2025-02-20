from src.ingest_data import download_from_s3, ingest_data
import logging


S3_FILES = {
    "products.csv": ['product_id','product_name','category','price'],
    "orders.csv": ['order_id', 'customer_id', 'order_date', 'total_amount'],
    "order_items.csv": ['order_item_id', 'order_id', 'product_id', 'quantity','price']
}

def main():
    """ Orquesta la descarga e ingestión """
    logging.info("Inicio del proceso de ingestión")

    # for filename in S3_FILES:
    #     file_path = download_from_s3(filename)
    #     if file_path:
    #         table_name = filename.replace(".csv", "").lower()
    #         ingest_data(file_path, table_name)
    
    for filename in S3_FILES:
        table_name = filename.replace(".csv", "").lower()
        ingest_data(f"data/{filename}", table_name, S3_FILES[filename])

    logging.info("Proceso finalizado")

if __name__ == "__main__":
    main()
