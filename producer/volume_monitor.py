import os
import time
import logging
import subprocess

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

KAFKA_DATA_DIR = "/var/lib/kafka/data" # Adjust for container path
THRESHOLD_GB = 2.0

def get_dir_size(path):
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            total_size += os.path.getsize(fp)
    return total_size

def main():
    logger.info("Starting volume validation monitor...")
    while True:
        try:
            # Note: This runs inside a container or where kafka data is accessible
            size_bytes = get_dir_size(KAFKA_DATA_DIR)
            size_gb = size_bytes / (1024 ** 3)
            logger.info(f"Current Kafka storage: {size_gb:.2f} GB")
            
            if size_gb >= THRESHOLD_GB:
                logger.info(f"TARGET REACHED: Kafka storage is {size_gb:.2f} GB (Threshold: {THRESHOLD_GB} GB)")
            
            # Check ingestion rate (placeholder for more complex logic)
            # In a real setup, we'd query Kafka metrics
            
        except Exception as e:
            logger.error(f"Error checking storage: {e}")
            
        time.sleep(600) # Check every 10 minutes

if __name__ == "__main__":
    main()
