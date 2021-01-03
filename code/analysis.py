from utils import *
import sys
from operator import add
import time
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

machine_events_file = '../data/machine_events/part-00000-of-00001.csv'


# Fields dictionaries for each file
machine_ev_f = {'time':0, 'machine_ID':1, 'event':2, 'platform_ID':3, 'CPUs':4, 'Memory':5}
task_ev_f = {'time':0, 'job_ID':2, 'task_index':3, 'scheduling_class':7, 'priority':8, 'event_type':5, 'machine_ID':4, 'req_RAM':10}
job_ev_f = {'time':0, 'job_ID':2, 'scheduling_class':5}
task_usage_f = {'job_ID':2, 'task_index':3, 'used_RAM':6, 'assigned_RAM':7}


# Start trace in microseconds
START_TRACE = 600000000 

EVICT=2
SCHEDULE=1

class Analyzer(object):
    """
    docstring
    """
    def __init__(self, nb_threads):
        self.utils = Utils()

        # Start a Spark Context using 'nb_threads' threads
        param_nb_threads = 'local[{}]'.format(nb_threads)
        sc_conf = SparkConf()

        sc_conf.set('spark.executor.memory', '32G')
        sc_conf.set('spark.executor.cores', '8')
        sc_conf.set('spark.driver.memory', '32G')
        sc_conf.set('spark.driver.maxResultSize', '32G')
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
        
        for i in range(-1, 499):
            # Generate the next file_name to be processed
            file_name = self.utils.get_next_file(i, 2)

            print('Processing file: {}'.format(file_name))

            job_events_RDD = self.read_file(file_name)

            # From the task_events RDD, create pairs of job_ID and scheduling class for each entry
            job_pairs = job_events_RDD.map(lambda x: (int(x[job_ev_f['job_ID']]), int(x[job_ev_f['scheduling_class']])))

            # Append the pairs(job_ID, sch_class) from each file to the acumulator and remove duplicates
            acc_job = acc_job.union(job_pairs)

            if (i + 2) % 100 == 0:
                acc_job = acc_job.distinct().collect()
            # acc_job = set(acc_job)

                acc_job = self.sc.parallelize(acc_job)

            # self.uncache(job_events_RDD)
        
        # acc_job = acc_job.distinct()

        # # Sort the pairs in ascending order by the job_ID
        # acc_job = acc_job.sortBy(lambda x: x[0])

        acc_tasks = self.sc.parallelize([])
        for i in range(-1, 499):
            # Generate the next file_name to be processed
            file_name = self.utils.get_next_file(i, 1)
            print('Processing file: {}'.format(file_name))

            task_events_RDD = self.read_file(file_name)

            # From the task_events RDD, create pairs of job_ID and task_index for each entry
            task_pairs = task_events_RDD.map(lambda x: (int(x[task_ev_f['job_ID']]), (int(x[task_ev_f['task_index']]),
                                                        int(x[task_ev_f['scheduling_class']]), int(x[task_ev_f['priority']]))))

            # Append the pairs(job_ID, task_index) from each file to the acumulator and remove duplicates
            acc_tasks = acc_tasks.union(task_pairs)

            if (i + 2) % 100 == 0:
                acc_tasks = acc_tasks.distinct().collect()
            # acc_tasks = set(acc_tasks)

                acc_tasks = self.sc.parallelize(acc_tasks)

        # acc_tasks = acc_tasks.distinct()

        acc_tasks = acc_tasks.map(lambda x: (x[0], (x[1][1], x[1][2])))
        

        acc_tasks_sums = acc_tasks.reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1]))
        acc_tasks_counts = self.sc.parallelize(list(acc_tasks.countByKey().items()))

        acc_tasks_join = acc_tasks_sums.join(acc_tasks_counts)

        acc_tasks_join = acc_tasks_join.map(lambda x: (x[0], (x[1][0][0]/x[1][1], x[1][0][1]/x[1][1])))


        # Delete the job_ID
        join_pairs = acc_job.join(acc_tasks_join).map(lambda x: x[1])

        # print(join_pairs[0])

        # join_pairs = self.sc.parallelize(join_pairs.fold([], lambda x, y: x+y))

        join_pairs_sums = join_pairs.reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1]))
        join_pairs_counts = self.sc.parallelize(list(join_pairs.countByKey().items()))
        join_pairs_join = join_pairs_sums.join(join_pairs_counts)
        join_pairs_join = join_pairs_join.map(lambda x: (x[0], (x[1][0][0]/x[1][1], x[1][0][1]/x[1][1]))).collect()

        print('join_pairs_join')
        print(join_pairs_join)


    def question5(self):
        """
        Do tasks with low priority have a higher probability of being evicted?
        """
        acc_tasks = self.sc.parallelize([])
        # Loop over all the 'task_event' files and get the useful info from each one of them.
        for i in range(-1, 499):
            # Generate the next file_name to be processed
            file_name = self.utils.get_next_file(i, 1)
            print('Processing file: {}'.format(file_name))
            task_events_RDD = self.read_file(file_name)

            # From the task_events RDD, create tuples of: task_index, priority, and event_type
            job_pairs = task_events_RDD.map(lambda x: (int(x[task_ev_f['task_index']]), (int(x[task_ev_f['priority']]), int(x[task_ev_f['event_type']]))))

            # Concatenate the tuples of the current file to the accumulator
            acc_tasks = acc_tasks.union(job_pairs).collect()

            # Keep an unique occurrence of each tuple
            acc_tasks = set(acc_tasks)

            # Load the previous resul in an RDD
            acc_tasks = self.sc.parallelize(acc_tasks)


        # For a certain key(task_index), keep only the task entry that has 'EVICT' event type
        # If none of them has the 'EVICT' type, keep any
        task_pairs = acc_tasks.reduceByKey(lambda x, y: compare(x,y))

        # Remove the 'task_index' from the tuple
        total_nb_entries_per_prio = task_pairs.map(lambda x: (x[1][0], x[1][1]))

        # Count how many tasks exist with a certain priority
        total_nb_entries_per_prio = self.sc.parallelize(list(total_nb_entries_per_prio.countByKey().items()))

        # Count how many EVICT task exist with a certain priority
        total_evicted_entries = task_pairs.filter(lambda x: x[1][1] == EVICT).map(lambda x: (x[1][0],x[1][1]))
        total_evicted_entries = self.sc.parallelize(list(total_evicted_entries.countByKey().items()))

        # Compute the percentage of evicted tasks, by computing the ratio betwen the #evicted and #tasks
        combined_entries = total_evicted_entries.join(total_nb_entries_per_prio).map(lambda x: (x[0], x[1][0] / x[1][1])).collect()

        print('combined_entries')
        print(combined_entries)


    def question6(self):
        """
        In general, do tasks from the same job run on the same machine?
        """
        acc_tasks = self.sc.parallelize([])
        for i in range(-1, 499):
            # Generate the next file_name to be processed
            file_name = self.utils.get_next_file(i, 1)
            print('Processing file: {}'.format(file_name))

            task_events_RDD = self.read_file(file_name)

            # From the task_events RDD, create pairs of job_ID and machine_ID for each entry
            acc_tasks = task_events_RDD.map(lambda x: (int(x[task_ev_f['job_ID']]), x[task_ev_f['machine_ID']]))

            # Since in the dataset there exist some tasks that don't have a machine_ID, we filter them out
            acc_tasks = acc_tasks.filter(lambda x: x[1] != '')

            acc_tasks = acc_tasks.union(acc_tasks).collect()
            # Keep an unique occurrence of each entry(remove the duplicates)
            acc_tasks = set(acc_tasks)
            acc_tasks = self.sc.parallelize(acc_tasks)
        
        # Count on how many machines the tasks from a job are runnning
        machines_for_a_job = self.sc.parallelize(list(acc_tasks.countByKey().items()))

        # Count how many jobs are running on a certain number of machines.
        # The pairs have the following form: (number_of_different_machines, number of jobs).
        # Each job is running on number_of_different_machines machines.
        machines = machines_for_a_job.map(lambda x: (x[1],1)).reduceByKey(lambda x, y: x+y).collect()

        print(machines)

    
    def question7(self):
        """
        docstring
        
        mean CPU usage rate
        canonical memory usage
        
        Ask the professor about the used CPU cores
                                mean disk used
        """
        acc_tasks = self.sc.parallelize([])
        for i in range(-1, 0):
            # Generate the next file_name to be processed
            file_name = self.utils.get_next_file(i, 1)
            print('Processing file: {}'.format(file_name))

            task_events_RDD = self.read_file(file_name)

            # From the task_usage RDD, create pairs of job_ID and task_index, event_type and mem_usage for each entry
            task_pairs = task_events_RDD.map(lambda x: (int(x[task_ev_f['job_ID']]), int(x[task_ev_f['task_index']]), int(x[task_ev_f['event_type']]), x[task_ev_f['req_RAM']]))

            task_pairs = task_pairs.filter(lambda x: x[3] != '')

            task_pairs = task_pairs.filter(lambda x: x[2] == SCHEDULE).map(lambda x: ((x[0], x[1]), float(x[3])))

            

            acc_tasks = acc_tasks.union(task_pairs)

            if (i + 2) % 100 == 0:
                acc_tasks = acc_tasks.distinct().collect()

                acc_tasks = self.sc.parallelize(acc_tasks)

        acc_task_usage = self.sc.parallelize([])
        for i in range(-1, 0):
            # Generate the next file_name to be processed
            file_name = self.utils.get_next_file(i, 3)
            print('Processing file: {}'.format(file_name))

            task_usage_RDD = self.read_file(file_name)

            # From the task_usage RDD, create pairs of job_ID and task_index, assigned_memory for each entry
            task_pairs = task_usage_RDD.map(lambda x: ((int(x[task_usage_f['job_ID']]), int(x[task_usage_f['task_index']])), float(x[task_usage_f['assigned_RAM']])))



            acc_task_usage = acc_task_usage.union(task_pairs)

        task_usage_sums = acc_task_usage.reduceByKey(lambda x, y: x + y)

        task_usage_counts = self.sc.parallelize(list(acc_task_usage.countByKey().items()))

        average_usage = task_usage_sums.join(task_usage_counts).map(lambda x: (x[0], x[1][0] / x[1][1]))


        join_req_and_used_mem = acc_tasks.join(average_usage).collect()

        print(join_req_and_used_mem[:5])
