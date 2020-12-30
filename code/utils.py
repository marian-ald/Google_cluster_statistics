from pyspark import SparkContext

def to_list(a):
    if type(a) is list:
        return a
    else:
        return [a]

def append(a, b):
    a.append(b)
    return a

def extend(a, b):
    a.extend(b)
    return a

class Utils(object):
    """
    Helper file fo different opperations that are not dirrectly related to
    data analysis using Spark.
    """

    def __init__(self):
        # Create a new Spark context
        # param_nb_threads = 'local[{}]'.format(nb_threads)
        # self.sc = SparkContext(param_nb_threads)
        # self.sc.setLogLevel("ERROR")
        self.file = None

    def read_file(self, file):
        """
        docstring
        """
        # wholeFile = self.sc.textFile(file)
        # return wholeFile
        pass


    def get_next_file(self, i, file_type):
        """
        docstring
        """
        i += 1
        file_no = ''
        if i < 10:
            file_no = '00{}'.format(i)
        elif i > 9 and i < 100:
            file_no = '0{}'.format(i)
        elif i > 99 and i < 500:
            file_no = '{}'.format(i)
        
        if file_type == 1:      # task event type
            return '../data/task_events/part-00{}-of-00500.csv'.format(file_no)
        elif file_type == 2:    # job event type
            return '../data/job_events/part-00{}-of-00500.csv'.format(file_no)

    def dump_in_file(self, string):
        if self.file == None:
            self.file = open("output.txt", "wa")
        self.file.write(string)
        self.file.write('\n')