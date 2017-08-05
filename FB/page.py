from lazy import lazy
import numpy as np
import pandas as pd
from FB.graphapi import GraphAPI


class Page(GraphAPI):
    def __init__(self, **kwargs ):
        super().__init__(**kwargs)
        self.id = kwargs.get('id', '')


    @lazy
    def stats(self):
        try:
            obj =  self.graph.get_object('{}?fields=likes.limit(0).summary(true),about'.format(self.id))
            return pd.Series({'likes':int(obj['likes']), 'about': obj['about']})
        except:
            return pd.Series()

