from whoosh.fields import (
        Schema, TEXT, KEYWORD, ID, STORED)
from whoosh.analysis import (
            StemmingAnalyzer, NgramTokenizer, 
                RegexTokenizer, StopFilter, 
                    LowercaseFilter, StandardAnalyzer,
                        FancyAnalyzer
                        )
from utils import Index, get_index_data

from pprint import pprint

analyzer = StandardAnalyzer()

schema = Schema(url=ID(stored=True,unique=True),
        title=TEXT(stored=True, analyzer=analyzer, field_boost=8),
        section=ID(stored=True, field_boost=10),
        text=TEXT(analyzer=analyzer, stored=True, field_boost=4),
        other_text=TEXT(analyzer=analyzer, field_boost=2))

index = Index(schema, "nairaland_index", "nairaland")
#data  = get_index_data("nairaland")
#index.index_data(data)
