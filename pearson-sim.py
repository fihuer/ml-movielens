#!/usr/bin/python

from Orange import misc
from dataset import MlDataset
from multiprocessing import Process, Manager, Pool
import multiprocessing
import time, math
from Queue import Full, Empty
from threading import Thread
from clint.textui import progress

#Definitions for Threading

def pearson(in_queue, out_queue):
    try:
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
                    #print "pearson", movie, x, y, ratings[x], ratings[y], xmean, ymean
            bottom_x = math.sqrt(bottom_x_sq)
            bottom_y = math.sqrt(bottom_y_sq)
            if((bottom_x*bottom_y) != 0):
                result = sum_top / (bottom_x*bottom_y)
            else:
                result = 0
            #print "result", result
            out_queue.put((x, y, result), block=True, timeout=1)
    except KeyboardInterrupt:
        return

def feed(args):
    try:
        xRange = args[0]
        users = args[1]
        dataset = args[2]
        in_queue=args[3]
        mean = args[4]

        for x in xRange:
            for y in users[:x]:
                xmean = mean[x]
                ymean = mean[y]
                filtereddata = {movie: {user:  dataset.data[movie][user] for user in dataset.data[movie] if (user == x or user == y)} for movie in dataset.data}
                filtereddata = {movie: filtereddata[movie] for movie in filtereddata if filtereddata[movie]}
                in_queue.put((x,y, xmean, ymean, filtereddata), block=True, timeout=10)
    except Full:
        print ''
        print "In Queue's full"
    except KeyboardInterrupt:
        return

#Main Class

class MlDistanceMatrix:
    def __init__(self, dataset=None):
        self.dataset=dataset
        self.matrixSize = 200
        self.feederLaunched = False
        self.nbCPU = multiprocessing.cpu_count()
        self.feederPool = multiprocessing.Pool(processes=self.nbCPU)

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


    def __startFeeder__(self):
        #thread = Thread(target=self.__feedWorkers__, args=())
        #thread.daemon = True
        #thread.start()
        chunks = tuple(self.__chunks__(self.matrixSize/self.nbCPU))
        maxx = 0
        for i in chunks:
            maxx = max(maxx, i[0][-1])
        self.matrixSize=maxx+1
        self.feederPool.map_async(feed, chunks)
        self.feederLaunched = True

    def __stopFeeder__(self):
        self.feederPool.terminate()
        self.feederPool.join()

    def __gatherResults__(self):
        while not self.feederLaunched:
            print "Feeder not launched yet"
            time.sleep(0.5)
        while self.out_queue.empty():
            #print "No results yet"
            time.sleep(0.5)
        matrix = misc.SymMatrix(self.matrixSize)
        to_do = pow(self.matrixSize,2)/2
        done = 0
        workersKilled = False
        with progress.Bar(label="Distance Matrix", expected_size=to_do) as bar:
            while True:
                try:
                    result = self.out_queue.get(block=True, timeout=2)
                    done += 1
                    #print result[0], result[1], self.matrixSize
                    matrix[result[0], result[1]] = result[2]
                    #print 'Distance matrix compuation {:10.4}% done\r\r'.format((done/to_do)*100),
                    if done <= to_do:
                        bar.show(done)
                except Empty:
                    break
        return matrix

    def kill(self):
        self.__stopFeeder__()
        self.__stopWorkers__()

    def __stopWorkers__(self):
        for p in self.__threads:
            p.terminate()
            p.join(1)

    def __startWorkers__(self, nbworkers=4):
        self.__manager = Manager()
        self.in_queue = self.__manager.Queue()
        self.out_queue = self.__manager.Queue()
        self.__threads = []
        for p in range(self.nbCPU):
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

class MlPredictor:
    def __init__(self, dataset):
        self.dataset = dataset
        self.matrixObj = MlDistanceMatrix(dataset)
        self.matrixThread = Thread(target=self.__buildMatrix__, args=())
        self.matrixThread.daemon = True
        self.matrixThread.start()
        self.buildingMatrix = True
        self.distanceMatrix = None

    def __buildMatrix__(self):
        self.distanceMatrix = self.matrixObj.matrix
        self.buildingMatrix = False

    def __joinMatrixThread__(self):
        self.matrixThread.join()

    def kill(self):
        self.matrixObj.kill()
        if self.buildingMatrix:
            self.matrixThread.join()

    def crossValidation(self):
        misses = 0
        predicted = 0
        for movie in self.dataset.data:
            for user in range(self.matrixObj.matrixSize):
                #print "########"
                #print "Predicting ", movie, " for ", user
                to_predict = self.dataset.data[movie].get(user, None)
                if to_predict != None:
                    predictedscore = int(self.predict(movie,user))
                    #print "predicted : "+str(predicted)
                    #print "real :", to_predict
                    #print "###"
                    predicted += 1
                    if predictedscore != to_predict:
                        misses +=1
        print "Misses/predicted : ", misses,'/',predicted
        print misses*100/float(predicted), "% missed"

    def distance(self, u1, u2):
        if self.buildingMatrix:
            self.__joinMatrixThread__()
        elif self.distanceMatrix == None:
            raise "Matrix Generation failed"
        return self.distanceMatrix[u1][u2]

    def predict(self, movie, user):
        #print "##",user,"##"
        xmean = self.dataset.mean[user]
        rsum = 0
        for otheruser in self.dataset.users(self.matrixObj.matrixSize):
            #print "Other :", otheruser
            otherscore = self.dataset.data[movie].get(otheruser, None)
            othermean = self.dataset.mean[otheruser]
            if otherscore != None:
                #print "Distance ",user, otheruser, self.distance(otheruser, user)
                rsum += (self.distance(otheruser, user))*(otherscore-othermean)
        return xmean + rsum

if __name__=="__main__":
    #f = open('matrix.txt', 'w')
 #   f.write(str(MlSimMatrix().matrix))
    dataset = MlDataset('dataset/ml-100k.txt')
    obj = MlPredictor(dataset)
    obj.crossValidation()
