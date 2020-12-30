from utils import *
import sys
from operator import add
import time
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

machine_events_file = '../data/machine_events/part-00000-of-00001.csv'


# Fields dictionaries for each file
machine_ev_f = {'time':0, 'machine_ID':1, 'event':2, 'platform_ID':3, 'CPUs':4, 'Memory':5}
task_ev_f = {'time':0, 'job_ID':2, 'task_index':3, 'scheduling_class':7, 'priority':8}
job_ev_f = {'time':0, 'job_ID':2, 'scheduling_class':5}


# Start trace in microseconds
START_TRACE = 600000000 

class Analyzer(object):
    """
    docstring
    """
    def __init__(self, nb_threads):
        self.utils = Utils()

        # Start a Spark Context using 'nb_threads' threads
        param_nb_threads = 'local[{}]'.format(nb_threads)
        sc_conf = SparkConf()

        sc_conf.set('spark.executor.memory', '8G')
        sc_conf.set('spark.executor.cores', '8')
        sc_conf.set('spark.driver.memory', '16G')
        sc_conf.set('spark.driver.maxResultSize', '8G')
        sc_conf.set('spark.sql.autoBroadcastJoinThreshol','-1')
        sc_conf.setMaster('local[*]')

        self.sc = SparkContext(param_nb_threads, conf=sc_conf)

        # conf = new SparkConf()
        #     .setMaster("local[2]")
        #     .setAppName("CountingSheep")

        # self.sc = SparkContext(param_nb_threads)
        # self.sc = SparkSession.builder().master("local[*]").config("spark.executor.memory", "70g").config("spark.driver.memory", "50g")\
        #         .config("spark.memory.offHeap.enabled", True)\
        #         .config("spark.memory.offHeap.size","16g")\
        #         .appName("sampleCodeForReference")\
        #         .getOrCreate()

        self.sc.setLogLevel("ERROR")

        # SparkConf().setMaster('local').setAppName('test').set('spark.local.dir', '/tmp/spark-temp')

    def read_file(self, file_name):
        # Read machine_events file
        file_rdd = self.sc.textFile(file_name)
        file_rdd = file_rdd.map(lambda x : x.split(','))

        # Keep the RDD in memory
        # file_rdd.cache()

        return file_rdd

    def uncache(self, rdd_name):
        if rdd_name.is_cached:
            rdd_name.unpersist()


    def question1(self):
        """
        What is the distribution of the machine according to their CPU capacity?
        """
        print('\nQuestion 1')

        machine_ev = self.read_file(machine_events_file)
        start = time.time()
        # Extract all unique entries by machine_ID from machine_events
        uniques_machines = machine_ev.map(lambda x: (x[machine_ev_f['machine_ID']], x)).reduceByKey(lambda x, _: x).map(lambda x: x[1])

        # Count how many machines exists for each type of 'CPUs'
        distribution = uniques_machines.map(lambda x: (x[machine_ev_f['CPUs']], 1)).reduceByKey(add)

        get_distribution = distribution.collect()
        end = time.time()
        print(get_distribution)
        print("Time: {}".format(end-start))
        self.uncache(machine_ev)

    def question2(self):
        """
        What is the percentage of computational power lost due to maintenance (a machine went
        offline and reconnected later)?
        """
        print('\nQuestion 2')

        machine_ev = self.read_file(machine_events_file)

        start = time.time()
        # Get the events that occur after the start of trace(600s) and that are not 'UPDATE'
        non_zero_ts = machine_ev.filter(lambda x: x[machine_ev_f['time']] != '0' and x[machine_ev_f['event']] != '2')

        # Get the last event for each machine, in order to decide if the last interval of each machine is an 'ADD' or 'REMOVE'
        last_events = non_zero_ts.map(lambda x: (x[machine_ev_f['machine_ID']], x)).reduceByKey(lambda x, y: x if (int(x[machine_ev_f['time']]) > int(y[machine_ev_f['time']])) else y)

        """
            Generate pairs (machine, timestamp) for each event, where 'timestamp' can have the values:
                -timestamp: if event_type == 'ADD'
                timestamp: if event_type == 'REMOVE'
                0 : if event_type == 'UPDATE
        """
        time_stamps = non_zero_ts.map(lambda x: (x[machine_ev_f['machine_ID']], -int(x[machine_ev_f['time']])) if (x[machine_ev_f['event']] == '0')
                            else (x[machine_ev_f['machine_ID']], int(x[machine_ev_f['time']])))

        # Sum of the timestamps
        sum_times = time_stamps.reduceByKey(add)

        # Get the last event from machine_events. I consider its timestamp as the end of the trace
        last_event_time = int(machine_ev.zipWithIndex().map(lambda x: (x[1],x[0])).max()[1][machine_ev_f['time']])
        total = last_event_time - START_TRACE

        # Create a list of pairs ((machine_ID, sum_time), last_event_type), to know if to add or to decrease the last interval time
        pairs = sum_times.zip(last_events)

        # If the last event is 'ADD', I add the last interval: the machine is running, otherwise I decrease: the machine is stopped
        final_times = pairs.map(lambda x: x[0][1] + last_event_time if x[1][1][machine_ev_f['event']] == '0' else x[0][1] - last_event_time)
        sum_final = final_times.reduce(add)

        # Compute the average time of running in microseconds
        sum_final = sum_final / len(last_events.collect())

        end = time.time()
        print("Time: {}".format(end-start))

        print('Percentage time UP = {}'.format(sum_final/total))
        print('Percentage time DOWN = {}'.format(1 - sum_final/total))
        self.uncache(machine_ev)

        
    def question3(self):
        """
            # On average, how many tasks compose a job?
        """    
        print('\nQuestion 3')

        # Accumulator for the information from all 500 task events files
        acc = self.sc.parallelize([])
        start = time.time()
        # Loop over the task-events files(500 in total)
        for i in range(-1, 499):
            # Generate the next file_name to be processed
            file_name = self.utils.get_next_file(i, 1)
            print('Processing file: {}'.format(file_name))

            task_events_RDD = self.read_file(file_name)

            # From the task_events RDD, create pairs of job_ID and task_index for each entry
            task_pairs = task_events_RDD.map(lambda x: (int(x[task_ev_f['job_ID']]), int(x[task_ev_f['task_index']])))

            # Append the pairs(job_ID, task_index) from each file to the acumulator and remove duplicates
            # acc = acc.union(task_pairs).distinct().collect()
            acc = acc.union(task_pairs).collect()

            acc = set(acc)

            acc = self.sc.parallelize(acc)

            # self.uncache(task_events_RDD)

        # Count how many tasks exist for each job
        no_tasks_for_a_job = acc.map(lambda x: (x[0],1)).reduceByKey(lambda x, y: x+y)

        # Compute the total number of jobs
        number_jobs = no_tasks_for_a_job.count()
        print('Total number of jobs: {}'.format(number_jobs))

        self.utils.dump_in_file('Total number of jobs: {}'.format(number_jobs))

        # Compute how many tasks are in total
        number_tasks = no_tasks_for_a_job.map(lambda x: x[1]).reduce(lambda x, y: x+y)
        print('Total number of tasks: {}'.format(number_tasks))
        self.utils.dump_in_file('Total number of tasks: {}'.format(number_tasks))

        # Mean = no_tasks/no_jobs
        average = float(number_tasks)/float(number_jobs)
        end = time.time()
        print("Time: {}".format(end-start))

        self.utils.dump_in_file("Time: {}".format(end-start))
        
        print('Average number of tasks/job: {}'.format(float(average)))
        self.utils.dump_in_file('Average number of tasks/job: {}'.format(float(average)))


    def question4(self):
        """
            # On average, how many tasks compose a job?
        """    
        print('\nQuestion 4')
        acc_job = self.sc.parallelize([])
        start = time.time()
        
        for i in range(-1, -1):
            # Generate the next file_name to be processed
            file_name = self.utils.get_next_file(i, 2)

            print('Processing file: {}'.format(file_name))

            job_events_RDD = self.read_file(file_name)

            # From the task_events RDD, create pairs of job_ID and scheduling class for each entry
            job_pairs = job_events_RDD.map(lambda x: (int(x[job_ev_f['job_ID']]), int(x[job_ev_f['scheduling_class']])))

            # Append the pairs(job_ID, sch_class) from each file to the acumulator and remove duplicates
            acc_job = acc_job.union(job_pairs).collect()

            acc_job = set(acc_job)

            acc_job = self.sc.parallelize(acc_job)

            self.uncache(job_events_RDD)
        
        # # Sort the pairs in ascending order by the job_ID
        # acc_job = acc_job.sortBy(lambda x: x[0])

        acc_tasks = self.sc.parallelize([])
        for i in range(-1, 1):
            # Generate the next file_name to be processed
            file_name = self.utils.get_next_file(i, 1)
            print('Processing file: {}'.format(file_name))

            task_events_RDD = self.read_file(file_name)

            # From the task_events RDD, create pairs of job_ID and task_index for each entry
            task_pairs = task_events_RDD.map(lambda x: (int(x[task_ev_f['job_ID']]), (int(x[task_ev_f['task_index']]),
                                                        int(x[task_ev_f['scheduling_class']]), int(x[task_ev_f['priority']]))))

            # Append the pairs(job_ID, task_index) from each file to the acumulator and remove duplicates
            acc_tasks = acc_tasks.union(task_pairs).collect()

            acc_tasks = set(acc_tasks)

            acc_tasks = self.sc.parallelize(acc_tasks)


        acc_tasks = acc_tasks.map(lambda x: (x[0], (x[1][1], x[1][2])))
        

        acc_tasks_sums = acc_tasks.reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1]))
        acc_tasks_counts = self.sc.parallelize(list(acc_tasks.countByKey().items()))

        acc_tasks_join = acc_tasks_sums.join(acc_tasks_counts)

        acc_tasks_join = acc_tasks_join.map(lambda x: (x[0], (x[1][0][0]/x[1][1], x[1][0][1]/x[1][1])))


        # Delete the job_ID
        join_pairs = acc_job.join(acc_tasks_join).map(lambda x: x[1])

        join_pairs = sc.parallelize(join_pairs.fold([], lambda x, y: x+y))

        join_pairs_sums = join_pairs.reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1]))
        join_pairs_counts = self.sc.parallelize(list(join_pairs.countByKey().items()))
        join_pairs_join = join_pairs_sums.join(join_pairs_counts)
        join_pairs_join = join_pairs_join.map(lambda x: (x[0], (x[1][0][0]/x[1][1], x[1][0][1]/x[1][1]))).collect()

        print('join_pairs_join')
        print(join_pairs_join)
