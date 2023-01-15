import boto3
import gzip
import shutil
from goose3 import Goose
import re

s3_client = boto3.client('s3',
                         aws_access_key_id="***REMOVED***",
                         aws_secret_access_key="***REMOVED***",
                         region_name="us-east-1"
                         )


def get_pages_from_common_crawl(domain, s3_client):
    # Utworzenie klienta dla usługi AWS S3


    # Pobranie listy obiektów (stron) z danej domeny w projekcie Common Crawl
    response = s3_client.list_objects_v2(Bucket='commoncrawl', Prefix='crawl-data/CC-MAIN-2022-49/segments/1669446708046.99/warc/CC-MAIN-20221126180719-20221126210719-00468.warc.gz')
    print(response)


    # Pobranie listy stron z odpowiedzi
    pages = response['Contents']

    # Pobranie zawartości stron za pomocą klienta S3
    pages_contents = []
    for page in pages:
        key = page['Key']
        #response = s3_client.get_object(Bucket='commoncrawl', Key=page['Key'])
        #pages_contents.append(response['Body'].read())
        #s3_client.download_file('commoncrawl', key, f"{key.split('/')[-1]}")

    return pages_contents


#with gzip.open('CC-MAIN-20221126180719-20221126210719-00468.warc.gz', 'rb') as f_in:
#    with open('CC-MAIN-20221126180719-20221126210719-00468.warc', 'wb') as f_out:
#        shutil.copyfileobj(f_in, f_out)


#pages = get_pages_from_common_crawl('onet.pl', s3_client)
#print(pages)



from warcio.archiveiterator import ArchiveIterator

with open('CC-MAIN-20221126180719-20221126210719-00468.warc', 'rb') as stream:
    for record in ArchiveIterator(stream):
        if record.rec_type == 'response':
            url = record.rec_headers.get_header('WARC-Target-URI')
            if 'serwisprawa' in url:
                print(url)
                html = record.content_stream().read()
                #g = Goose()
                #article = g.extract(raw_html=html)
                #print(article.title)
                #print(article.cleaned_text)
                from newspaper import Article
                article = Article("")
                article.download(input_html=html)
                article.parse()
                txt = article.text
                txt = re.sub(r"\n{2,}", "\n", txt)
                print(txt)
                #break