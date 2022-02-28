import xapian
import sys
import json
import csv
import time
from xapian import BM25PlusWeight
import re

WORD = re.compile("\w+")
STOPWORDS = ['a', 'about', 'above', 'after', 'again', 'against', 
            'ain', 'all', 'am', 'an', 'and', 'any', 'are', 'aren', "aren't", 
            'as', 'at', 'be', 'because', 'been', 'before', 'being', 'below', 
            'between', 'both', 'but', 'by', 'can', 'could', 'couldn', 
            "couldn't", 'd', 'did', 'didn', "didn't", 'do', 
            'does', 'doesn', "doesn't", 'doing', 'don', "don't", 'down', 
            'during', 'each', 'few', 'for', 'from', 'further', 'had', 'hadn', 
            "hadn't", 'has', 'hasn', "hasn't", 'have', 'haven', "haven't", 'having', 
            'he', "he'd", "he'll", "he's", 'her', 'here', "here's", 'hers', 'herself', 
            'him', 'himself', 'his', 'how', "how's", 'i', "i'd", "i'll", "i'm", "i've", 'if', 
            'in', 'into', 'is', 'isn', "isn't", 'it', "it's", 'its', 'itself', 'just', 
            "let's", 'll', 'm', 'ma', 'me', 'mightn', "mightn't", 'more',          
            'most', 'mustn', "mustn't", 'my', 'myself', 
            'needn', "needn't", 'no', 'nor', 'not', 'now', 'o', 
            'of', 'off', 'on', 'once', 'only', 'or', 'other', 'ought', 
            'our', 'ours', 'ourselves', 'out', 'over', 'own', 
            're', 's', 'same', 'shan', "shan't", 'she', "she'd", 
            "she'll", "she's", 'should', "should've", 'shouldn', "shouldn't", 
            'so', 'some', 'such', 't', 'than', 'that', "that'll", 
            "that's", 'the', 'their', 'theirs', 
            'them', 'themselves', 'then', 'there',
            "there's", 'these', 'they', "they'd", "they'll",
            "they're", "they've", 'this', 'those', 'through', 'to', 'too', 
            'under', 'until', 'up', 've', 'very', 'was', 'wasn', "wasn't", 
            'we', "we'd", "we'll", "we're", "we've", 'were', 'weren', 
            "weren't", 'what', "what's", 'when', "when's", 'where', 
            "where's", 'which', 'while', 'who', "who's", 'whom', 'why', 
            "why's", 'will', 'with', 'won', "won't", 'would', 
            'wouldn', "wouldn't", 'y', 'you', "you'd", "you'll",                                                                                    "you're", "you've", 'your', 'yours', 'yourself', 'yourselves']


class Result:
    def __init__(self, query, results, count, current_page, page_size, time, correction=""):
        self.results = results
        self.query   = query
        self.count   = count
        self.time    = time
        self.current_page = current_page + 1
        self.page_size  = page_size
        self.correction = correction
        self.r1 = current_page * page_size
        self.r2 = self.r1 + len(results)	

    @property
    def _pages(self):
        count = self.count // self.page_size
        if self.count % self.page_size != 0:
            count += 1

        return list(range(1,count+1))

    @property
    def pages(self):
        pagination = set([1])
        current_page = self.current_page
        max_p = 10
        pagenum = self.count // self.page_size 
 
        if current_page < max_p:
            pagination.update(range(2, max_p+1))
        else:
            if current_page < pagenum:
                pagination.update(range(current_page-3, current_page+3))
            else:
                pagination.update(range(current_page-max_p, current_page+1))
        pagination.add(pagenum)
        pagination = sorted(list(pagination))
        pagination = [p for p in pagination if p <= pagenum]

        new_pagination = []
        last = 0
        for p in pagination:
            if last+1 != p:
                new_pagination.append("…")
            new_pagination.append(p)
            last = p
                
        return new_pagination    
 
    def __iter__(self):
        for hit in self.results:
            hit["highlight"] = {}
            title_h = "".join(x[0] for x in self.highlight(hit.get("title","")))
            text_h  = "".join(x[0] for x in self.highlight(hit.get("text",""), big_text=True)[:2]) 
            hit["highlight"]["title"] = title_h
            hit["highlight"]["text"]  = text_h
            yield hit

    def highlight(self, text, big_text=False, words_per_fraction=20):
        text = text.lower()
        q = self.query.split()
        fractions,fraction = [], ""
        i = 0
        highlights = []

        if big_text:
            for token in text.split():
                fraction += token + " "
                i += 1
                if i >= words_per_fraction:
                    fractions.append(fraction.strip() + "…")
                    fraction = "…"
                    i = 0
            else:
                fractions.append(fraction.strip())		
        else:
            fractions = [text]

        for fraction in fractions:
            count = 0
            for i in q:
                i = i.strip()
                if WORD.fullmatch(i):
                    count += fraction.count(i)
                    fraction = re.sub("(\s)({})".format(i)," <b>{}</b>".format(i), fraction)
                    fraction = re.sub("^({})".format(i),"<b>{}</b>".format(i), fraction)

                    #fraction = fraction.replace(i, "<b>" + i + "</b>")
            highlights.append((fraction.strip(),count))

        return sorted(highlights, key=lambda x:x[1], reverse=True)
		
class Index:
    def __init__(self, dbpath):
        self.dbpath = dbpath

    def add_doc(self,docs):
        db = xapian.WritableDatabase(self.dbpath, xapian.DB_CREATE_OR_OPEN)
        termgenerator = xapian.TermGenerator()
        termgenerator.set_stemmer(xapian.Stem("en"))
        
        for fields in docs: 
            text = fields.get("text",u'')
            title = fields.get("title", u'')
            identifier = fields.get("e_url", u'')
            sec = fields.get("sec", u'')
            more_text = fields.get("other_text", u"")

            doc = xapian.Document()
            termgenerator.set_document(doc)

            termgenerator.index_text(title, 1, 'S')
            termgenerator.index_text(text, 1, "XD")
            termgenerator.index_text(more_text, 1, "X")
            termgenerator.index_text(sec, 1, 'S')

            termgenerator.index_text(title)
            termgenerator.increase_termpos()
            termgenerator.index_text(text)
            termgenerator.increase_termpos()
            termgenerator.index_text(more_text)
            
            doc.add_value(0, sec)

            doc.set_data(json.dumps(fields))
                
            idterm = u"Q" + identifier
            doc.add_boolean_term(idterm)
            db.replace_document(idterm, doc)
    
    def _cquery(self, query, field, op):

        query = query.lower()
        terms = WORD.findall(query)
        queries = []
        
        for term in terms:
            if term not in STOPWORDS:
                queries.append("{}:{}".format(field,term))
        return " {} ".format(op).join(queries)

    def cquery(self, query):
        fields  = [("title", "OR"), ("text", "OR")]
        queries = [] 
        for field, op in fields:
            queries.append("({})".format(self._cquery(query, field, op)))

        return " AND ".join(queries)

    def search(self, querystring, offset=0, pagesize=15):
        db = xapian.Database(self.dbpath)

        queryparser = xapian.QueryParser()
        queryparser.set_stemmer(xapian.Stem("en"))
        queryparser.set_stemming_strategy(queryparser.STEM_SOME)

        queryparser.add_prefix("title", "S")
        queryparser.add_prefix("text", "XD")

        _querystring = self.cquery(querystring)
        # print(_querystring)
        query = queryparser.parse_query(_querystring)
        
        start_time = time.time()
        
        enquire = xapian.Enquire(db)
        enquire.set_weighting_scheme(BM25PlusWeight())
        enquire.set_query(query)

        end_time = time.time()
        query_time = "{:.2}".format(end_time - start_time)

        matches = []
        results = []
        m_results = enquire.get_mset(offset,pagesize)
        count     = m_results.get_matches_estimated()

        for match in m_results:
            #print(dir(match), match.collapse_count)
            fields = json.loads(match.document.get_data().decode("UTF-8"))
            '''
            print("{rank}: #{docid} {title}".format(
                rank=match.rank + 1,
                docid=match.docid,
                title=fields.get("title", u"")))
            '''
            fields["rank"]  = match.rank + 1
            fields["docid"] = match.docid
            fields["highlight"] = fields.get("title", u'')
            results.append(fields)
            
            # matches.append(match.docid)
        return Result(querystring, results, count, offset, pagesize, query_time)

        #support.log_matches(querystring, offset, pagesize, matched)

#Index("myindex").search("watch")






