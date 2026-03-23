import argparse
import glob
import logging
import os

import boto3
from botocore.exceptions import BotoCoreError, ClientError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def parse_args():
    """Parse command line arguments and return the input folder and destination."""
    parser = argparse.ArgumentParser(
        description="Upload results*.csv files to an S3 bucket/prefix."
    )
    parser.add_argument(
        "input_folder",
        help="Folder containing the results*.csv files"
    )
    parser.add_argument(
        "destination",
        help="S3 destination in the form bucket/prefix/, e.g. ds2002-bae7kx/book-analysis/"
    )
    args = parser.parse_args()
    return args.input_folder, args.destination


def upload(input_folder, destination):
    """Upload all results*.csv files from the input folder to the S3 destination."""
    try:
        s3 = boto3.client("s3", region_name="us-east-1")

        parts = destination.split("/", 1)
        bucket = parts[0]
        prefix = ""
        if len(parts) > 1:
            prefix = parts[1].strip("/")

        pattern = os.path.join(input_folder, "results*.csv")
        files = glob.glob(pattern)

        if not files:
            logger.error("No files matching results*.csv were found in %s", input_folder)
            return False

        for file_path in files:
            file_name = os.path.basename(file_path)

            if prefix:
                key = prefix + "/" + file_name
            else:
                key = file_name

            s3.upload_file(file_path, bucket, key)
            logger.info("Uploaded %s to s3://%s/%s", file_name, bucket, key)

        return True

    except (ClientError, BotoCoreError, OSError) as e:
        logger.error("Upload failed: %s", e)
        return False


def main():
    """Run argument parsing and upload files, then log overall success or failure."""
    input_folder, destination = parse_args()
    success = upload(input_folder, destination)

    if success:
        logger.info("Script completed successfully.")
    else:
        logger.error("Script completed with errors.")


if __name__ == "__main__":
    main()
