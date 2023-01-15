import requests
from tqdm import tqdm
import os
from lm_dataformat import Archive
import json
import glob
import sys
from tool.simple_cache import save_cache, get_cache, get_content
from tool.link_analyser import get_all_links, filter_links
from tool.warc_analyser import download_and_save

base_dir = os.path.dirname(os.path.realpath(__file__))
cache_dir = os.path.join(base_dir, "cache")
txt_dir = os.path.join(base_dir, "txt")

if not os.path.exists(cache_dir):
    os.makedirs(cache_dir)

if not os.path.exists(txt_dir):
    os.makedirs(txt_dir)

mode = 1

if len(sys.argv) < 2:
    print("Please specify a mode")
    sys.exit()

try:
    mode = int(sys.argv[1])
    if mode < 1 or mode > 3:
        print("Please specify a mode between 1 and 3")
        sys.exit()
except:
    print("Please specify a mode between 1 and 3")
    sys.exit()


if mode == 1:

    print("Mode #1: Downloading indexes and url to pages")

    indexes = json.loads(get_content("http://index.commoncrawl.org/collinfo.json", 5, cache_dir, "index"))

    if not indexes:
        print("Indexes not found!")
        sys.exit()

    txt_datasets = glob.glob('./*.json')

    for f in txt_datasets:


        data = None
        with open(f, 'r') as jf:
            data = json.load(jf)

        if data:
            print("     Processing dataset: " + data.get("name",""))
            websites = data.get("websites",[])

            for website in websites:
                print("         Processing website: " + website.get("domain",""))
                domain = website.get("domain","")
                for index in indexes:
                    index_url = index.get("cdx-api","")
                    url = index_url + "?url=" + domain + "&output=json"
                    get_content(url, 20, cache_dir, "page_urls")
                    

if mode == 2:

    print("Mode #2: Generate urls to WARC files")

    all_links = get_all_links(cache_dir, "page_urls")

    txt_datasets = glob.glob('./*.json')

    for f in txt_datasets:

        data = None
        with open(f, 'r') as jf:
            data = json.load(jf)

        if data:

            domain = data.get("domain","")
            filtred_links = filter_links(all_links, domain)

            filtred_links_txt = json.dumps(filtred_links, indent=4)
            with open(os.path.join(cache_dir, data.get("name","") +  ".warcs"), 'w') as jf:
                jf.write(filtred_links_txt)
            print("File WARCS " + os.path.join(cache_dir, data.get("name","") + ".warcs") + " saved!")

if mode == 3:

    print("Mode #3: Download and analize WARC")

    txt_datasets = glob.glob('./*.json')

    for f in txt_datasets:

        data = None
        with open(f, 'r') as jf:
            data = json.load(jf)

        if data:

            if os.path.exists(os.path.join(cache_dir, data.get("name","") + ".warcs")):
                print("File WARCS " + os.path.join(cache_dir, data.get("name","") + ".warcs") + " already exists!")
                with open(os.path.join(cache_dir, data.get("name","") + ".warcs"), 'r') as jf:
                    filtred_links = json.load(jf)
                download_and_save(filtred_links, txt_dir, base_dir)
            else:
                print("File WARCS "  + os.path.join(cache_dir, data.get("name","") + ".warcs") + " not found!")

