#!/usr/bin/python

from Orange import core
from dataset import MlDataset
from multiprocessing import Process, Manager, Pool
import time, math
from Queue import Full, Empty
from threading import Thread

def pearson(in_queue, out_queue):
    while True:
        try:
            args = in_queue.get(block=True, timeout=1)
        except Empty:
            #print "In Queue empty"
            return
        x = args[0]
        y = args[1]
        xmean = args[2]
        ymean = args[3]
        data = args[4]

        bottom_x_sq = 0
        bottom_y_sq = 0
        sum_top = 0
        for movie, ratings in data.iteritems():
            if ((x in ratings) and (y in ratings)):
                sum_top += (ratings[x]-xmean)*(ratings[y]-ymean)
                bottom_x_sq += pow(ratings[x]-xmean, 2)
                bottom_y_sq += pow(ratings[y]-ymean, 2)
        bottom_x = math.sqrt(bottom_x_sq)
        bottom_y = math.sqrt(bottom_y_sq)
        if((bottom_x*bottom_y) != 0):
            result = sum_top / (bottom_x*bottom_y)
        else:
            result = 0
        out_queue.put((x, y, result), block=True, timeout=1)

def feed(args):
    xRange = args[0]
    users = args[1]
    dataset = args[2]
    in_queue=args[3]
    mean = args[4]

    for x in xRange:
        for y in users[x:]:
            xmean = mean[x]
            ymean = mean[y]
            filtereddata = {movie: {user:  dataset.data[movie][user] for user in dataset.data[movie] if (user == x or user == y)} for \
movie in dataset.data}
            filtereddata = {movie: filtereddata[movie] for movie in filtereddata if filtereddata[movie]}
            try:
                in_queue.put((x,y, xmean, ymean, filtereddata), block=True, timeout=10)
            except Full:
                print ''
                print "In Queue's full"

class MlSimMatrix:
    def __init__(self):
        self.dataset = MlDataset("dataset/ml-100k.txt")
        self.matrixSize = 200
        self.feederLaunched = False

    @property
    def matrix(self):
        self.__startWorkers__()
        self.__startFeeder__()
        matrix = self.__gatherResults__()
        self.__stopWorkers__()
        return matrix
        
    def __chunks__(self, csize):
        for i in xrange(0, self.matrixSize, csize):
            yield [self.dataset.users(self.matrixSize)[i:i+csize], self.dataset.users(self.matrixSize), self.dataset ,self.in_queue, self.dataset.mean]
    

    def __startFeeder__(self, nbfeeders=4):
        #thread = Thread(target=self.__feedWorkers__, args=())
        #thread.daemon = True
        #thread.start()
        p = Pool(nbfeeders)
        chunks = tuple(self.__chunks__(self.matrixSize/nbfeeders))
#        print chunks
        p.map_async(feed, chunks)
        self.feederLaunched = True

    def __gatherResults__(self):
        while not self.feederLaunched:
            print "Feeder not launched yet"
            time.sleep(0.5)
        while self.out_queue.empty():
            print "No results yet"
            time.sleep(0.5)
        matrix = core.SymMatrix(self.matrixSize)
        to_do = pow(self.matrixSize,2)/float(2)
        done = 0
        while True:
            try:
                result = self.out_queue.get(block=True, timeout=2)
                done += 1
            except Empty:
                break
            matrix[result[0], result[1]] = result[2]
            if done%100==0:
                print str(done/to_do)+'\r\r',
        return matrix

    def __stopWorkers__(self):
        for p in self.__threads:
            p.terminate()
            p.join(1)

    def __startWorkers__(self, nbworkers=4):
        self.__manager = Manager()
        self.in_queue = self.__manager.Queue()
        self.out_queue = self.__manager.Queue()
        self.__threads = []
        for p in range(nbworkers):
            p = Process(target=pearson, args=(self.in_queue,self.out_queue))
            p.start()
            self.__threads.append(p)
            
        
    def __feedWorkers__(self):
        print "Building pairs"
        users = self.dataset.users(self.matrixSize)
        to_do = pow(len(users),2)/2
        still_to_do = to_do
        for x in users:
            for y in users[x:]:
                still_to_do -= 1
                if still_to_do >= 0:
                    print'Feeder :'+str(100-(still_to_do/float(to_do))*100)+'%\r\r',
                xmean = self.dataset.mean[x]
                ymean = self.dataset.mean[y]
                filtereddata = self.dataset.filter(x,y)
                try:
                    self.in_queue.put((x,y, xmean, ymean, filtereddata), block=True, timeout=10)
                    if not self.feederLaunched:
                        self.feederLaunched = True
                except Full:
                    print ''
                    print "In Queue's full"
        self.feederLaunched = False
        print ''
        print "Done"
    

if __name__=="__main__":
    f = open('matrix.txt', 'w')
 #   f.write(str(MlSimMatrix().matrix))
    mat = str(MlSimMatrix().matrix).replace(',', '\t').replace('))','').replace('(','')
    f.write(mat)
    f.close()
