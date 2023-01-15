import hashlib
import os
import requests

def save_cache(cache, key, url, content):

    hash = hashlib.md5(url.encode()).hexdigest()
    cache_file = os.path.join(cache, hash + "." + key)
    with open(cache_file, 'w') as f:
        f.write(content)

    return True

def get_cache(cache, key, url):
    hash = hashlib.md5(url.encode()).hexdigest()
    cache_file = os.path.join(cache, hash + "." + key)
    if os.path.exists(cache_file):
        print("Cache found: " + cache_file)
        with open(cache_file, 'r') as f:
            return f.read()
    else:
        print("Cache not found: " + cache_file)
    return None

def get_content(url, samples, cache, key=None):

    if cache:
        content = get_cache(cache, key, url)
        if content:
            return content

    for i in range(samples):
        r  = requests.get(url)
        if r.status_code == 200:
            if cache:
                save_cache(cache, key, url, r.text)
            print("Response received: " + url)
            return r.text
 
        print("Waiting for response... #", i, url, r.status_code)

    return None