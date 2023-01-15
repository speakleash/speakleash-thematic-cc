import glob
import json

def get_all_links(cache_dir, key):

    all_links = {}
    for f in glob.glob(cache_dir + "/*." + key):
        with open(f, 'r') as jlf:
            lines = jlf.readlines()
            for line in lines:
                js = json.loads(line)
                if js.get("status", "0") == "200":
                    if js.get("mime","") == "text/html":
                        file_name = js.get("filename","")
                        if file_name not in all_links:
                            all_links[file_name] = []
                        all_links[file_name].append({"url" : js.get("url","")})

    return all_links

def check_url(url, domain):
    return True

def filter_links(all_links, domain):
    
    filtred_links = {}
    for key in all_links:
        urls = all_links[key]
        for url in urls:
            u = url.get("url", "")
            if check_url(u, domain):
                if key not in filtred_links:
                    filtred_links[key] = []
                filtred_links[key].append({"url" : u}) 

    return filtred_links