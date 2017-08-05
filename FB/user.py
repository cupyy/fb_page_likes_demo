from lazy import lazy
import numpy as np
import pandas as pd
from FB.graphapi import GraphAPI


class User(GraphAPI):
    def __init__(self, **kwargs ):
        super().__init__(**kwargs)
        self.id = kwargs.get('id', 'me')


    @lazy
    def friends_obj(self):
        return self.graph.get_object('{}/friends'.format(self.id))


    @lazy
    def all_friends(self):
        return self.get_all(self.friends_obj)


    @lazy
    def feed_obj(self):
        return self.graph.get_object('{}/feed'.format(self.id))


    @lazy
    def all_feeds(self):
        return self.get_all(self.feed_obj)


    @lazy
    def likes_obj(self):
        return self.graph.get_object('{}/likes'.format(self.id))


    @lazy
    def all_likes(self):
        return self.get_all(self.likes_obj)


    @lazy
    def likes_pages(self):
        return self.all_likes


