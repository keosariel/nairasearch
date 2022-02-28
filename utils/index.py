from   whoosh.filedb.filestore import FileStorage
from   whoosh.fields import (
    Schema, TEXT, KEYWORD, 
    ID, STORED
)
from whoosh.analysis import StemmingAnalyzer
import whoosh.index as index
from whoosh.qparser import MultifieldParser, QueryParser
from whoosh import qparser
from whoosh.spelling import ListCorrector, MultiCorrector
from whoosh import highlight

import os

class Result:
    
    def __init__(self, results, count, suggestions=[], 
                    correction="", more_like_this=[]):
        
        self.results = results
        self.suggestions    = suggestions
        self.more_like_this = more_like_this
        self.correction     = correction
        self.count          = count
    
    def __iter__(self):
        for res in self.results:
            yield res
    
    def review(self):
        print("=============== (%s) RESULTS =================" % (self.count))
        if self.correction:
            print("Did you mean? ", self.correction)
        for x in self.results:
            print(x.get("title"))
            print(x.get("url"))
            print()

        print()
        print("=============== MORE LIKE THIS =================")
        for x in self.more_like_this[:5]:
            print(x.get("title"))


class Index:
    def __init__(self, schema, index_dir, indexname):
        """Creates an Index or Loads an Index if
        it exists already.
        
        :param index_dir: Index parent directory
        :type  index_dir: `str`
        :param indexname:  Index Name
        :type  indexname:  `str`
        
        :return: `None`
        """
        
        if not os.path.exists(index_dir):            
            os.makedirs(index_dir)
            
        self.storage = FileStorage(index_dir)
        
        if not index.exists_in(index_dir, indexname=indexname):
            self.ix = self.storage.create_index(
                schema, indexname=indexname
            )
        else:
            self.ix = self.storage.open_index(
                            indexname=indexname)
        
        self.schema = schema
        
        self.default_group  = qparser.OrGroup.factory(0.9)
        self.default_parser = None
        #self._corrector     = ListCorrector(self.load_word_list())
        
    @property
    def fields(self):
        return self.schema.names()

    def index_data(self, data):
        with self.ix.writer() as writer: 
            for d in data:
                writer.add_document(**{k:d.get(k, "") for k in self.fields})
    
    def search(self, fields, query, page=1, pagelen=20):
        """Searches the index for the given `query` 
        
        :param query: text to be searched for
        :type query: `str`
        :param fields: list of fields to search
        :type fields: List[str]
        :param page: Current result page
        :type page: `int`
        :param pagelen: Amount of results per page
        :type pagelen: `int`
        
        :return: :class:`Results` object
        """
        qstring = query
        query   = query.encode('ascii', 'ignore') # .encode('utf-8')
        
        if not self.default_parser:
            self.default_parser = MultifieldParser(
                fields, schema=self.schema,  group=self.default_group)
        
        query = self.default_parser.parse(query)
        
        with self.ix.searcher() as searcher:
            # corrected = searcher.correct_query(query, 
            #                                 qstring, 
            #                                 correctors=self.field_corrector(fields, searcher), 
            #                                 maxdist=1)
            # corrected2 = searcher.correct_query(query, 
            #                                 qstring, 
            #                                 correctors={f:self._corrector for f in fields}, 
            #                                 maxdist=1)
            correction = ""
            # if corrected.query != query:
            #     correction = corrected.format_string(highlight.HtmlFormatter())
            # elif corrected2.query != query:
            #      correction = corrected2.format_string(highlight.HtmlFormatter())
                
            # if correction:
            #     query = corrected.query
                
            results  = searcher.search_page(query, page, 
                                        pagelen=pagelen)
            
            query_results = self._clean_results(fields, results, correction=correction)             
            return query_results 
        
    def field_corrector(self, fields, searcher):
        return { f: searcher.corrector(f) 
                                for f in fields }
        
    def _clean_result(self, fields, result, highlight=False):
        r = dict(**result)
        r["score"] = result.score
        
        r["highlight"] =  {}
        
        if highlight:
            for f in fields:
                r["highlight"][f] = result.highlights(f)
            
        return r     
    
    def _clean_results(self, fields, results, correction=""):
        """Cleans results and provides a better usable 
        result properties
        
        NOTE: This was done because when the results are out
        of scope the data aren't readable anymore because the 
        searcher is closed
        
        :param fields: fields that were searched
        :param results: Search/Query results
        
        :return: Clean search result
        """
        count  = len(results)
        new_result = []

        for r in results:
            new_result.append(self._clean_result(fields, r, highlight=True))
        
        query_results = Result(new_result, 
                                count=count, 
                                correction=correction,
                                more_like_this=self._get_more_like_this(fields, results))
        
        return query_results  
    
    def _get_more_like_this(self, fields, results):
        more_like_this = []

        for hit in results[:4]:
            for r in hit.more_like_this(fields[0]):
                if r not in results:
                    more_like_this.append(self._clean_result(fields, r))
                    
        more_like_this = sorted(more_like_this, key=lambda x:x.get("score", 0), reverse=True)
        
        return more_like_this    
    
    def load_word_list(self):
        words = []
        with open("words.txt", "r", errors="igonre", encoding="UTF-8") as fp:
            words = fp.readlines()
        
        return words
