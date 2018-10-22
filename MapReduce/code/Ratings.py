from mrjob.job import MRJob
from mrjob.step import MRStep

class Ratings(MRJob):
    def steps(self):
        return [MRStep(mapper = self.ratingsMapper, reducer = self.ratingsReducer)]
    
    def ratingsMapper(self, _, line):
        (userID, movieID, rating, timestamp) = line.split('\t')
        yield rating,1
    
    def ratingsReducer(self, key, values):
        yield key, sum(values)
        
if __name__ == '__main__':
    Ratings.run()
