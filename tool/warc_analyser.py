import boto3
import gzip
import shutil
from newspaper import Article
import re
import os
from warcio.archiveiterator import ArchiveIterator
import hashlib
import time

from botocore.config import Config

def check_local_txt(urls, txt_dir):
    found = True
    for item in urls:
        url = item.get("url", "")
        hash = hashlib.md5(url.encode()).hexdigest()
        file_name = os.path.join(txt_dir, hash + ".txt")
        if not os.path.exists(file_name):
            found = False
            return found
    return found

def url_exitst(url, urls):

    for item in urls:
        u = item.get("url", "")
        if u == url:
            return True
    return False

def save_txt(url, text, txt_dir):

    hash = hashlib.md5(url.encode()).hexdigest()
    file_name = os.path.join(txt_dir, hash + ".txt")
    with open(file_name, 'w') as f:
        f.write(text)

def download_and_save(filtred_links, txt_dir, base_dir):

    s3_client = None
    counter = 0

    config = Config(
    retries = {
        'max_attempts': 10,
        'mode': 'standard'
    }
    )

    for key in filtred_links:

        urls = filtred_links[key]
        local_file_name_gz = os.path.join(base_dir, key.split('/')[-1])
        local_file_name_warc = local_file_name_gz.replace(".gz", "")

        found = check_local_txt(urls, txt_dir)
        if found:
            continue

        if not s3_client:
            s3_client = boto3.client('s3',
                                    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
                                    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
                                    region_name="us-east-1",
                                    config=config
                                    )

        response = s3_client.list_objects_v2(Bucket='commoncrawl', Prefix=key)
        items = response['Contents']

        if not os.path.exists(local_file_name_warc):
            for item in items:
                item_key = item['Key']
                print("Downloading: " + item_key)
                
                for i in range(15):
                    
                    try:
                        s3_client.download_file('commoncrawl', item_key, local_file_name_gz)
                        break
                    except Exception as e:
                        print("Waiting for response... #", i, item_key)
                        print(e)

                    time.sleep(90)

                break

        if os.path.exists(local_file_name_gz):
            print("Decompressing: " + local_file_name_gz)
            with gzip.open(local_file_name_gz, 'rb') as f_in:
                with open(local_file_name_warc, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
            os.remove(local_file_name_gz)

        if os.path.exists(local_file_name_warc):
            print("Analysis: " + local_file_name_warc)

            with open(local_file_name_warc, 'rb') as stream:
                for record in ArchiveIterator(stream):
                    if record.rec_type == 'response':
                        url = record.rec_headers.get_header('WARC-Target-URI')
                        if url_exitst(url, urls):
                            print("Saving: " + url)
                            html = record.content_stream().read()
                            article = Article("")
                            article.download(input_html=html)
                            article.parse()
                            txt = article.text
                            txt = re.sub(r"\n{2,}", "\n", txt)
                            save_txt(url, txt, txt_dir)
                            counter += 1

            os.remove(local_file_name_warc)
            print("Saved: " + str(counter) + " files. " + local_file_name_warc)

        else:
            print("File not found: " + local_file_name_gz)

