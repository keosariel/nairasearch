from .db import DB
import sqlalchemy
from sqlalchemy import Integer, Text
import threading
import base64
from tldextract import extract
import urllib.parse
from bs4 import BeautifulSoup
import requests
import logging
import os
import re

logging.basicConfig(format='[%(levelname)s] : [%(asctime)s] : %(message)s', datefmt='%d-%b-%y %H:%M:%S', 
                    level=logging.DEBUG)

URL_TABLE = ("url",
    [   
        {"name" : "id", "type" : Integer, "unique":True, "nullable":False, "primary_key":True},
        {"name" : "url", "type" : Text, "unique":True},
        {"name" : "state","type" : Integer}
    ]
)

global_lock = threading.Lock()

class Cronus:

    def __init__(self, **config):
        """
        Initiates url database and few configs

        :param pass: links to reject (out of scope)
        :param seed: links to start with
        :param limit_urls: Amount of urls to crawl at a time
        """

        logging.debug('Instantiating Cronus')

        fields = ("pass", "seed", "working_dir")
        if not all(map(lambda x:x in config, fields)):    
            raise Exception("Expected these arguements: %s" % (fields))

        self._pass = [re.compile(r) for r in config.get("pass", [])]
        self.url   = None
        self.seed  = config.get("seed")
        self.working_dir = config.get("working_dir")
        self.site_data   = os.path.join(self.working_dir, "site")
        self.db_path     = os.path.join(self.working_dir, "url.db")
        self.recent  = config.get("recent",[])        
        self.limit_urls  = config.get("limit_urls", 10)
        self.new_links = []
        self.init_db()

        self.crawled = []
        self._urls     = []

    def run(self):
        """
        Starts gathering links and storing site downloaded data
        Note: Does this in threads, for speed
        """

        logging.debug('Add url tasks')

        seed_urls = self.get_urls()
        
        tasks     = []

        for url in seed_urls:
            link = url.get("url")
            
            t = threading.Thread(target=self.save_url, args=(link,))
            t.start()
            tasks.append(t)
        
        for t in tasks:
            t.join()
        
        self.save_all()
            
    def init_db(self):
        """
        Creates the neccessary table to store 
        links to crawl and has been crawled
        """
        logging.debug('Instantialazing Site Database [%s]' % (self.db_path))

        if not os.path.exists(self.working_dir):
            os.makedirs(self.working_dir)

        if not os.path.exists(self.site_data):
            os.makedirs(self.site_data)
    
        self.db  = DB("sqlite:///%s" % (self.db_path))
        self.db.create_table(*URL_TABLE)
        self.url = self.db.table("url")

        for url in self.seed:
            self.add_url([{"url":url, "state":0}])
        
        self.add_recent()

    def add_recent(self):
        headers = {
            'Accept': '*/*',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36'}

        if self.recent:
            for url in self.recent:
                try:
                    response = requests.get(url, headers=headers)
                except Exception:
                    pass

                logging.debug('[%s] Requesting' % (url))

                soup = BeautifulSoup(response.text)
                links = soup.select("a")

                for a in links: 
                    href = a.attrs.get("href")
                    if href:
                        href = self.clean_url(href)
                        if self.can_add(href):
                            # print(url, href)
                            self.new_links.append((url, href))
    def get_urls(self):
        """
        Gathers links to crawl from the table
        if they've not been crawled

        :param limit: amount of links to return

        :return: list of urls
        """ 
        limit = self.limit_urls

        logging.debug('Gathering [%s] seed urls' % (limit))

        filters = [("state","==",0)] 
        results  = (
            self.url.select()
            .where(*filters)
            .execute()
        )
        results = list(results)
        print("################################ {} URLS ##########################".format(len(results)))
        result = []
        i = 0
        for row in results[::-1]:
            url = dict(row.items())
            if self.can_add(url.get('url')):
                result.append(url)
                i += 1

            if i >= limit:
                break

        #result = [ dict(row.items()) for i, row in enumerate(result) if i < limit ]

        return result

    def can_add(self, url):
        for regex in self._pass:
            if regex.fullmatch(url):
                return False

        return True
    
    def _add_url(self, *new_urls):
        """
        Adds url to the `URL` table in the url database
        if they don't exists

        :param url: a valid url/link
        """
        urls = []
        for origin, url in new_urls:
            url = url.strip()

            if not url.startswith("#"):
                if url.startswith("/"):
                    u = urllib.parse.urlparse(origin)
                    addon = u.netloc

                    if u.scheme:
                        addon = u.scheme+"://"+addon

                    url = addon + url

                url = self.clean_url(url)
                if not url:
                    continue

                eu = extract(url)
                o_eu = extract(origin)

                if eu.domain == o_eu.domain and self.can_add(url):
                    if url not in self._urls:
                        self._urls.append(url)
                        urls.append({"url":url, "state":0})

        
        return urls
    
    def add_url(self, urls):
        for url in urls:
            try:
                self.url.insert(**url)
            except sqlalchemy.exc.IntegrityError:
                pass

    def update_urls(self, urls, state=2):
        """
        Signifies that a url has been crawled

        :param url: valid link/url to crawl
        :param state: an integer that signifies that a link 
                      has been crawled
        """
        
        for url in urls:
            filters = [("url", "==", url)]

            (
                self.url.update()
                .where(*filters)
                .values(state=state)
                .execute()
            )

    def clean_url(self, url):
        """
        Removes query parameters for url i.e `ID` and `query-parameters`
        target

        Note: since we'd only crawl specific sites it's neccessary

        Example:
        --------
        >>> clean_url('https://www.example.com#abc?a=b')
        >>> 'https://www.example.com'

        :return: new url `string`
        """
        try:
            out =  urllib.parse.urljoin(
                url, urllib.parse.urlparse(url).path
            )
        except Exception as err:
            return None

        return out

    def save_url(self, url):
        """
        Get downloads html data from url and gathers all links 
        in the page to add to `not crawled` list sort off :)
        """

        #if not self.can_add(url):
        #    return

        response = None
        
        headers = {
            'Accept': '*/*',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36'}

        try:
            response = requests.get(url, headers=headers)
        except requests.exceptions.InvalidSchema:
            self.crawled.append(url)
        except requests.exceptions.MissingSchema:
            self.crawled.append(url)
            
        logging.debug('[%s] Requesting' % (url))


        if response:
            if response.status_code in range(200,300):
                logging.debug('[%s] Requesting succeeded <%s>' % (url, response.status_code))

                soup = BeautifulSoup(response.text)
                links = soup.select("a")

                more_urls = []

                for a in links: 
                    href = a.attrs.get("href")
                    if href:
                        more_urls.append(href)
                
                self.save(url=url, html=response.text, more_urls=more_urls)

                #elif response.status_code in range(500,600):
                #pass
            else:
                self.crawled.append(url)
            
    def save(self, url, html,more_urls=[]):
        """
        Saves downoad html data and save new
        urls to be crawled

        :param url: crawled url
        :param html: text/html data
        :param more_urls: New urls to be crawled
        """

        logging.debug('[%s] Saving html data' % (url))

        for u in more_urls: 
            self.new_links.append((url, u))
        
        self.crawled.append(url)

        eu = extract(url)
        fp = os.path.join(self.site_data, eu.domain)

        if not os.path.exists(fp):
            os.mkdir(fp)

        fn = base64.b64encode(url.encode("UTF-8"))
        fn = fn.decode("UTF-8")

        filename = os.path.join(fp, fn)
        
        with open(filename, "w", encoding="UTF-8", errors="ignore") as fp:
            fp.write(html)

    def save_all(self):
        print("------------------------------------------------------------")
        logging.debug('Saving all!!!!')
        
        self.update_urls(self.crawled)

        urls = self._add_url(*self.new_links)
        self.add_url(urls)

        self.db.session.commit()

        logging.debug("Crawled: [%s]" % (len(self.crawled)))
        logging.debug("New links: [%s]" % (len(self.new_links)))

 
