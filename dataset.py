#!/usr/bin/python

from Orange import data
import json


class MlDataset ():
    def __init__(self, datasetpath):
        self.__mean = None
        self.__users = None
        self.__table = data.Table(datasetpath)
        self.__preprocess__()

    def __preprocess__(self):
        self.data = {}
        for inst in self.__table:
            s = int(inst["item id"])
            x = int(inst["user id"])
            note = int(inst["rating"])
            if (self.data.get(s) != None):
                self.data[s][x]=note
            else:
                self.data[s] = {x: note}

    def filter(self, x, y):
        res = {movie: {user:  self.data[movie][user] for user in self.data[movie] if (user == x or user == y)} for \
movie in self.data}
        return {movie: res[movie] for movie in res if res[movie]}

    @property
    def mean(self):
        if self.__mean == None:
            user_ratings = {}
            nb_ratings = {}
            for inst in self.__table:
                user_id = int(inst['user id'])
                user_ratings[user_id] = user_ratings.get(user_id, 0) +float(inst['rating'])
                nb_ratings[user_id] = nb_ratings.get(user_id,0)+1
            for id in user_ratings:
                user_ratings[id] = user_ratings[id]/nb_ratings[id]
            self.__mean = user_ratings
        return self.__mean

    def users(self, limit=0):
        if self.__users == None:
            if limit == 0:
                self.__users = self.mean.keys()
            else:
                self.__users = self.mean.keys()
                self.__users.sort()
                self.__users = self.__users[:limit]
        return self.__users

    @property
    def size(self):
        return len(self.users())

if __name__=="__main__":
    print json.dumps(MlDataset("dataset/ml-100k.txt").mean, sort_keys=True, indent=4)
