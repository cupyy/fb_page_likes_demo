from lazy import lazy
import numpy as np
import pandas as pd
import facebook
import requests
import json


class GraphAPI():
    def __init__(self, **kwargs ):
        super().__init__()
        self.access_token = kwargs.get('access_token', '')


    @lazy
    def graph(self):
        return facebook.GraphAPI(self.access_token, version='2.5')


    @lazy
    def content(self):
        return pd.Series(self.get_obj(self.id))


    def get_obj(self, id=None, metadata=1):
        id = self.id if self.id else id
        return self.graph.get_object(id, metadata=metadata)


    ########################################################################
    # To retrieve all data by searching all next
    # specify limit to a number to cap the number of return records
    ########################################################################
    def get_all(self, obj=None, limit=float('inf')):
        if ( obj is None ): return pd.DataFrame()

        df = pd.DataFrame()
        while(len(df) < limit):
            try:
                df = pd.concat([df, pd.DataFrame(obj['data'])], ignore_index=True)
                obj=requests.get(obj['paging']['next']).json()
            except KeyError:
                ########################################################################
                # If no more pages (['paging']['next']), break from the loop
                ########################################################################
                break

        return df if limit == float('inf') else df[:limit]


    def search(self, query='', category='page', limit=float('inf')):
        data = {'q': query, 'type': category}

        if limit < float('inf'):
            data['limit'] = limit

        return self.graph.request("search", data )


        


