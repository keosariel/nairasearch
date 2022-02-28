from bs4 import BeautifulSoup
import glob
import os
import base64
from pprint import pprint
import re
import json
from htmldate import find_date

EC = re.compile("[a-z]+(/[a-z]+)?")

def get_site_files(dir, p="*"):
    if os.path.exists(dir):
        return sorted(glob.glob(os.path.join(dir, "*")), key=os.path.getmtime, reverse=True)

def get_config(fn, n=0):
    if n > 0:
        with open(fn, "w") as fp:
            fp.write(str(n))
        
    else:
        try:        
            with open(fn, "r") as fp:
                j = fp.read()
                n = int(j.strip())
        except Exception as err:
            print(err)
    
    return n

def get_sec(text):
    text = text.lower()
    t   = [x.strip() for x in text.split("-") if x.strip()]
    sec = (text, "")
    
    if len(t) >= 3:
        if t[-1] == "nairaland":
            if EC.fullmatch(t[-2].strip()):
                sec = ("".join(t[:-2]),t[-2].strip())
                
    elif text.split()[-1] == "profile":
        sec = (text, "profile")
        
    return sec

def extract_data(e_url, text):
	
    date = "" # find_date(text)    
    soup = BeautifulSoup(text)
    title = soup.select_one("h2")
    images = soup.select('div.narrow img')
    
    images = [i.attrs.get("src") for i in images]
    images = [i for i in images if i.startswith("http")]
    
    if not title:
        title = soup.select_one("title")
    
    if not title:
        return
    
    title = title.text.strip()
    
    content = soup.select("div.narrow")
    
    main_text  = ""
    other_text = []
    
    # if not content:
    #     content = soup.select("p")
    
    if content:
        main_text = content[0].text.strip()
        for c in content[1:]:
            other_text.append(c.text.strip())
    
    url = (base64.b64decode(e_url)
           .decode("utf-8"))
    
    title, sec = get_sec(title)
    
    if sec == "profile":
        tables = soup.select("table")
        if len(tables) >= 3:    
            main_text = tables[2].text
            
    data = {
        "title" : title,
        "url"   : url,
        "e_url" : e_url,
        "text" : main_text,
        "sec"  : sec,
        "other_text"  : ''.join(other_text),
        "images" : images,
        "date" :date
    }
    
    # print(title, sec)
    
    return data
    
def get_index_data(name, limit=10):
    d = "%(name)s/site/%(name)s" % {"name":name}
    files = get_site_files(d)

    data = []
    n = get_config("index.config")
    k = 0
    
    for i, f in enumerate(files[n:]):
        with open(f, "r") as fp:
            text = fp.read()
            
            e_data = extract_data(os.path.basename(f), text)
            if e_data:
                data.append(e_data)
        
        n += 1
        k += 1
        print(i)
        if k >= limit:
            break

    get_config("index.config", n)
        
    return data
