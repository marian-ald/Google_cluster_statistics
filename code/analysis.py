from utils import *
import sys
from operator import add
import time
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

EXE_MEMORY= 0
EXE_CORE= 0
DV_MEMOTY= 0
DV_CORE= 0
DV_MAX= 0

NUM_FILES= 0
PERCENTAGE= 0

machine_events_file = '../data/machine_events/part-00000-of-00001.csv'


# Fields dictionaries for each file
machine_ev_f = {'time':0, 'machine_ID':1, 'event':2, 'platform_ID':3, 'CPUs':4, 'Memory':5}
task_ev_f = {'time':0, 'job_ID':2, 'task_index':3, 'scheduling_class':7, 'priority':8, 'event_type':5, 'machine_ID':4, 'req_RAM':10}
job_ev_f = {'time':0, 'job_ID':2, 'scheduling_class':5}
task_usage_f = {'time_start':0, 'time_end':1, 'job_ID':2, 'task_index':3, 'used_RAM':6, 'assigned_RAM':7}


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

        sc_conf.set('spark.executor.memory', EXE_MEMORY+'M')
        sc_conf.set('spark.executor.cores', EXE_CORE)
        sc_conf.set('spark.driver.memory', DV_MEMOTY+'M')
        sc_conf.set('spark.executor.instances', 4)
        sc_conf.set('spark.driver.cores', DV_CORE)
        sc_conf.set('spark.driver.maxResultSize', DV_MAX+'M')
        sc_conf.set('spark.sql.autoBroadcastJoinThreshol','-1')
        # sc_conf.setMaster('local[*]')

        self.sc = SparkContext(param_nb_threads, conf=sc_conf)
        self.sc.setLogLevel("ERROR")

        # a = self.sc._conf.get('spark.executor.instances')
        # print('nb executor instances = {}'.format(a))

        # a = self.sc._conf.get('spark.executor.cores')
        # print('spark.executor.cores = {}'.format(a))

        # sys.exit()
        # self.utils.format_output_files()

        # Number of the current question
        self.nb_q = 1


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
        What is the distribution of the machines according to their CPU capacity?
        """
        print('\nQuestion 1')
        self.nb_q = 1

        machine_ev = self.read_file(machine_events_file)
        start = time.time()
        # Extract all unique entries by machine_ID from machine_events
        uniques_machines = machine_ev.map(lambda x: (x[machine_ev_f['CPUs']], x[machine_ev_f['machine_ID']])).distinct()

        # Count how many machines exists for each type of 'CPUs'
        distribution = uniques_machines.map(lambda x: (x[0], 1)).reduceByKey(add).collect()

        end = time.time()

        self.utils.dump_in_file(distribution, self.nb_q)
        self.utils.dump_in_file("Time: {}".format(end-start), self.nb_q)
        # self.uncache(machine_ev)

    def question2(self):
        """
        What is the percentage of computational power lost due to maintenance (a machine went
        offline and reconnected later)?
        """
        print('\nQuestion 2')
        self.nb_q = 2

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


        self.utils.dump_in_file("Time: {}".format(end-start), self.nb_q)
        self.utils.dump_in_file('Percentage time UP = {}'.format(sum_final/total), self.nb_q)
        self.utils.dump_in_file('Percentage time DOWN = {}'.format(1 - sum_final/total), self.nb_q)
        
        self.uncache(machine_ev)

        
    def question3(self):
        """
            # On average, how many tasks compose a job?
        """    
        print('\nQuestion 3')
        self.nb_q = 3

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
            acc = acc.union(task_pairs)
            # acc = set(acc)

            # acc = self.sc.parallelize(acc)

            if (i + 2) % 100 == 0:
                acc = acc.distinct().collect()

                acc = self.sc.parallelize(acc)

        # Count how many tasks exist for each job
        no_tasks_for_a_job = acc.map(lambda x: (x[0],1)).reduceByKey(lambda x, y: x+y)

        # Compute the total number of jobs
        number_jobs = no_tasks_for_a_job.count()
        self.utils.dump_in_file('Total number of jobs: {}'.format(number_jobs), self.nb_q)

        # Compute how many tasks are in total
        number_tasks = no_tasks_for_a_job.map(lambda x: x[1]).reduce(lambda x, y: x+y)
        self.utils.dump_in_file('Total number of tasks: {}'.format(number_tasks), self.nb_q)

        # Mean = no_tasks/no_jobs
        average = float(number_tasks)/float(number_jobs)
        end = time.time()

        self.utils.dump_in_file("Time: {}".format(end-start), self.nb_q)
        self.utils.dump_in_file('Average number of tasks/job: {}'.format(float(average)), self.nb_q)


    def question4(self):
        """
           Relationship between the scheduling class of a job, the scheduling class of its tasks, and their priority
        """
        print('\nQuestion 4')
        self.nb_q = 4

        acc_job = self.sc.parallelize([])
        start = time.time()

        # Extracting the needed information from Job event table
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

        # # Sort the pairs in ascending order by the job_ID
        # acc_job = acc_job.sortBy(lambda x: x[0])

        # Extracting the needed information from Task event table
        acc_tasks = self.sc.parallelize([])
        for i in range(-1, 499):
            # Generate the next file_name to be processed
            file_name = self.utils.get_next_file(i, 1)
            print('Processing file: {}'.format(file_name))

            task_events_RDD = self.read_file(file_name)

            # From the task_events RDD, create pairs of job_ID and task_index,scheduling_class,priority for each entry
            task_pairs = task_events_RDD.map(lambda x: (int(x[task_ev_f['job_ID']]), (int(x[task_ev_f['task_index']]),
                                                        int(x[task_ev_f['scheduling_class']]), int(x[task_ev_f['priority']]))))


            # Append the pairs(job_ID, task_index) from each file to the acumulator and remove duplicates
            acc_tasks = acc_tasks.union(task_pairs)

            if (i + 2) % 100 == 0:
                acc_tasks = acc_tasks.distinct().collect()

                acc_tasks = self.sc.parallelize(acc_tasks)

        # Create pairs of job_ID and scheduling_class,priority
        acc_tasks = acc_tasks.map(lambda x: (x[0], (x[1][1], x[1][2])))

        acc_tasks_sums = acc_tasks.reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1]))
        # Counting the number of tasks
        acc_tasks_counts = self.sc.parallelize(list(acc_tasks.countByKey().items()))

        acc_tasks_join = acc_tasks_sums.join(acc_tasks_counts)
        # Calculating the average of scheduling_class and priority
        acc_tasks_join = acc_tasks_join.map(lambda x: (x[0], (float(x[1][0][0])/float(x[1][1]), float(x[1][0][1])/float(x[1][1]))))

        # Delete the job_ID
        join_pairs = acc_job.join(acc_tasks_join).map(lambda x: x[1])

        # join_pairs = self.sc.parallelize(join_pairs.fold([], lambda x, y: x+y))

        join_pairs_sums = join_pairs.reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1]))
        join_pairs_counts = self.sc.parallelize(list(join_pairs.countByKey().items()))

        # Joining both results and creating a pairs of scheduling_class of jobs and average scheduling_class of tasks, average priority
        join_pairs_join = join_pairs_sums.join(join_pairs_counts)
        join_pairs_join = join_pairs_join.map(lambda x: (x[0], (float(x[1][0][0])/float(x[1][1]), float(x[1][0][1])/float(x[1][1])))).collect()

        end = time.time()
        self.utils.dump_in_file("Time: {}".format(end-start), self.nb_q)
        self.utils.dump_in_file(join_pairs_join, self.nb_q)

        self.utils.save_object(join_pairs_join, 4, 'class_prio')


    def question5(self):
        """
            Do tasks with low priority have a higher probability of being evicted?
        """
        print('\nQuestion 5')
        self.nb_q = 5

        # Accumulator for the information from all 500 task events files
        acc_tasks = self.sc.parallelize([])
        start = time.time()

        # Loop over all the 'task_event' files and get the useful info from each one of them.
        for i in range(-1, 499):
            # Generate the next file_name to be processed
            file_name = self.utils.get_next_file(i, 1)
            print('Processing file: {}'.format(file_name))
            task_events_RDD = self.read_file(file_name)

            # From the task_events RDD, create tuples of: task_index, priority, and event_type
            job_pairs = task_events_RDD.map(lambda x: (int(x[task_ev_f['task_index']]), (int(x[task_ev_f['priority']]), int(x[task_ev_f['event_type']]))))

            # Concatenate the tuples of the current file to the accumulator
            acc_tasks = acc_tasks.union(job_pairs)

            # Keep an unique occurrence of each entry(remove the duplicates)
            if (i + 2) % 100 == 0:
                acc_tasks = acc_tasks.distinct().collect()
                # Load the previous result in an RDD
                acc_tasks = self.sc.parallelize(acc_tasks)

            # acc_tasks = set(acc_tasks)
            # acc_tasks = self.sc.parallelize(acc_tasks)
        acc_tasks = acc_tasks.distinct().collect()
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
        combined_entries = total_evicted_entries.join(total_nb_entries_per_prio).map(lambda x: (x[0], float(x[1][0]) /float(x[1][1]))).collect()
        end = time.time()

        self.utils.dump_in_file("Time: {}".format(end-start), self.nb_q)
        self.utils.dump_in_file(combined_entries, self.nb_q)

        self.utils.save_object(combined_entries, 5, 'evict_probab')


    def question6(self):
        """
            In general, do tasks from the same job run on the same machine?
        """
        print('\nQuestion 6')
        self.nb_q = 6

        # Accumulator for the information from all 500 task events files
        acc_tasks = self.sc.parallelize([])
        start = time.time()

        for i in range(-1, int(NUM_FILES)):
            # Generate the next file_name to be processed
            file_name = self.utils.get_next_file(i, 1)
            print('Processing file: {}'.format(file_name))

            task_events_RDD = self.read_file(file_name)

            # From the task_events RDD, create pairs of job_ID and machine_ID for each entry
            acc_tasks = task_events_RDD.map(lambda x: (int(x[task_ev_f['job_ID']]), x[task_ev_f['machine_ID']]))

            # Since in the dataset there exist some tasks that don't have a machine_ID, we filter them out
            acc_tasks = acc_tasks.filter(lambda x: x[1] != '')

            acc_tasks = acc_tasks.union(acc_tasks)

            # Keep an unique occurrence of each entry(remove the duplicates)
            if (i + 2) % int(PERCENTAGE) == 0:
                acc_tasks = acc_tasks.distinct().collect()

                acc_tasks = self.sc.parallelize(acc_tasks)


        # Count on how many machines the tasks from a job are runnning
        machines_for_a_job = self.sc.parallelize(list(acc_tasks.countByKey().items()))

        # Count how many jobs are running on a certain number of machines.
        # The pairs have the following form: (number_of_different_machines, number of jobs).
        # Each job is running on number_of_different_machines machines.
        nb_jobs_on_nb_machines = machines_for_a_job.map(lambda x: (x[1],1)).reduceByKey(lambda x, y: x+y).collect()

        para_nb_jobs_on_nb_machines = self.sc.parallelize(nb_jobs_on_nb_machines)
        # Compute weighted average of the machines and jobs, in order to obtain on how many machines a job runs
        # in average
        weighted_sum = para_nb_jobs_on_nb_machines.map(lambda x: x[0] * x[1]).reduce(lambda x, y: x+y)
        sum_jobs = para_nb_jobs_on_nb_machines.map(lambda x: x[1]).reduce(lambda x, y: x+y)

        weighted_average =  weighted_sum/sum_jobs

        end = time.time()

        self.utils.dump_in_file("Time: {}".format(end-start), self.nb_q)
        self.utils.dump_in_file(nb_jobs_on_nb_machines, self.nb_q)
        self.utils.dump_in_file("weighted_average = {}".format(weighted_average), self.nb_q)
        self.utils.save_object(nb_jobs_on_nb_machines, 6, 'jobs_per_dif_machines')


    def question7(self):
        """
        Are the tasks that request the more resources the one that consume the more resources?
        
        mean CPU usage rate
        canonical memory usage
        
        TODO: Ask the professor about the used CPU cores
                                mean disk used
        """
        print('\nQuestion 7')
        self.nb_q = 7

        # Accumulator for the information from all 500 task events files
        acc_tasks = self.sc.parallelize([])
        start = time.time()

        for i in range(-1, int(NUM_FILES)):
            # Generate the next file_name to be processed
            file_name = self.utils.get_next_file(i, 1)
            print('Processing file: {}'.format(file_name))

            task_events_RDD = self.read_file(file_name)

            # From the task_events RDD, create pairs of job_ID and task_index, event_type and mem_usage for each entry
            task_pairs = task_events_RDD.map(lambda x: (int(x[task_ev_f['job_ID']]), int(x[task_ev_f['task_index']]), int(x[task_ev_f['event_type']]), x[task_ev_f['req_RAM']]))

            # Removing the entries that do not have requested RAM
            task_pairs = task_pairs.filter(lambda x: x[3] != '')
            # Filtering the scheduled event type for tasks
            task_pairs = task_pairs.filter(lambda x: x[2] == SCHEDULE).map(lambda x: ((x[0], x[1]), float(x[3])))

            acc_tasks = acc_tasks.union(task_pairs)

            if (i + 2) % int(PERCENTAGE) == 0:
                acc_tasks = acc_tasks.distinct().collect()
                acc_tasks = self.sc.parallelize(acc_tasks)

        acc_tasks = acc_tasks.distinct()
        acc_task_usage = self.sc.parallelize([])

        for i in range(-1, int(NUM_FILES)):
            # Generate the next file_name to be processed
            file_name = self.utils.get_next_file(i, 3)
            print('Processing file: {}'.format(file_name))

            task_usage_RDD = self.read_file(file_name)

            # From the task_usage RDD, create pairs of job_ID and task_index, assigned_memory for each entry
            task_pairs = task_usage_RDD.map(lambda x: ((int(x[task_usage_f['job_ID']]), int(x[task_usage_f['task_index']])), float(x[task_usage_f['assigned_RAM']])))

            acc_task_usage = acc_task_usage.union(task_pairs)

            if i % 5 == 0:
                acc_task_usage = acc_task_usage.distinct().collect()                
                acc_task_usage = self.sc.parallelize(acc_task_usage)

        # Do the summation for assigned_RAM
        task_usage_sums = acc_task_usage.reduceByKey(lambda x, y: x + y)

        task_usage_counts = self.sc.parallelize(list(acc_task_usage.countByKey().items()))
        # Get the average for assigned_memory
        average_usage = task_usage_sums.join(task_usage_counts).map(lambda x: (x[0], float(x[1][0]) /float( x[1][1])))

        # Joining the requested RAM by used_RAM
        join_req_and_used_mem = acc_tasks.join(average_usage)

        # Remove the (job_id, task_index) tuples from the RDD
        req_and_used_mem = join_req_and_used_mem.map(lambda x: x[1]).collect()

        print(req_and_used_mem[:5])

        end = time.time()
        self.utils.dump_in_file("Time: {}".format(end-start), self.nb_q)
        self.utils.dump_in_file(req_and_used_mem, self.nb_q)
        self.utils.save_object(req_and_used_mem, 7, 'req_and_used_mem')


    def question8(self):
        """
        Is there a relation between the amount of resource consumed by tasks and their priority?
        """
        print('\nQuestion 8')
        self.nb_q = 8
        
        # Accumulator for the information from all 500 task events files
        acc_tasks = self.sc.parallelize([])
        start = time.time()

        for i in range(-1, int(NUM_FILES)):
            # Generate the next file_name to be processed
            file_name = self.utils.get_next_file(i, 1)
            print('Processing file: {}'.format(file_name))

            task_events_RDD = self.read_file(file_name)

            # From the task_events RDD, create pairs of job_ID, task_index and priority for each entry
            task_pairs = task_events_RDD.map(lambda x: ((int(x[task_ev_f['job_ID']]), int(x[task_ev_f['task_index']])), int(x[task_ev_f['priority']])))

            # task_pairs = task_pairs.filter(lambda x: x[3] != '')

            # task_pairs = task_pairs.filter(lambda x: x[2] == SCHEDULE).map(lambda x: ((x[0], x[1]), float(x[3])))

            acc_tasks = acc_tasks.union(task_pairs)

            if (i + 2) % int(PERCENTAGE) == 0:
                acc_tasks = acc_tasks.distinct().collect()
                acc_tasks = self.sc.parallelize(acc_tasks)

        acc_tasks = acc_tasks.distinct().collect()
        acc_tasks = self.sc.parallelize(acc_tasks)

        print('Processed task_events files')

        acc_task_usage = self.sc.parallelize([])

        for i in range(-1, int(NUM_FILES)):
            # Generate the next file_name to be processed
            file_name = self.utils.get_next_file(i, 3)
            print('Processing file: {}'.format(file_name))

            task_usage_RDD = self.read_file(file_name)

            # From the task_usage RDD, create pairs of job_ID and task_index, assigned_memory for each entry
            task_pairs = task_usage_RDD.map(lambda x: ((int(x[task_usage_f['job_ID']]), int(x[task_usage_f['task_index']])), float(x[task_usage_f['assigned_RAM']])))

            acc_task_usage = acc_task_usage.union(task_pairs)

            if i % 5 == 0:
                acc_task_usage = acc_task_usage.distinct().collect()                
                acc_task_usage = self.sc.parallelize(acc_task_usage)

        # acc_task_usage = acc_task_usage.collect()
        # acc_task_usage = self.sc.parallelize(acc_task_usage)

        print(' Finished task_usage for loop Q8')

        # Doing the summation of assigned_memory for each pairs (job_ID, task_index) as a key
        task_usage_sums = acc_task_usage.reduceByKey(lambda x, y: x + y)

        task_usage_counts = self.sc.parallelize(list(acc_task_usage.countByKey().items()))

        # Calculating the average of assigned_memory for each task
        average_usage = task_usage_sums.join(task_usage_counts).map(lambda x: (x[0], float(x[1][0]) /float( x[1][1])))

        # Joining the tasks by average_usage RDD
        join_req_and_used_mem = acc_tasks.join(average_usage)

        # Removing the duplicates and keeping the average_usage of RAM
        join_req_and_used_mem = join_req_and_used_mem.distinct().map(lambda x: x[1])

        # Compute the sums of average RAM for each priority
        sums_ram_for_prio = join_req_and_used_mem.reduceByKey(lambda x, y: x + y)

        count_keys = self.sc.parallelize(list(join_req_and_used_mem.countByKey().items()))

        average_usage = sums_ram_for_prio.join(count_keys).map(lambda x: (x[0], float(x[1][0]) /float( x[1][1]))).collect()

        end = time.time()
        self.utils.dump_in_file("Time: {}".format(end-start), self.nb_q)
        self.utils.dump_in_file(average_usage, self.nb_q)
        self.utils.save_object(average_usage, 8, 'used_ram_per_prio')


    def question9(self):
        """
        correlations between peaks of high resource consumption on some machines and task eviction events?
        """
        print('\nQuestion 9')
        self.nb_q = 9

        # Accumulator for the information from the task usage files
        acc_tasks = self.sc.parallelize([])
        start = time.time()

        for i in range(-1, int(NUM_FILES)):
            # Generate the next file_name to be processed
            file_name = self.utils.get_next_file(i, 1)
            print('Processing file: {}'.format(file_name))

            task_events_RDD = self.read_file(file_name)

            # From the task_events RDD, create pairs of timestamps and event_type for each entry
            task_pairs = task_events_RDD.map(lambda x: (int(x[task_ev_f['time']]), int(x[task_ev_f['event_type']])))

            # Filtering out the evicted tasks
            task_pairs = task_pairs.filter(lambda x: x[1] == EVICT)

            # Calculating the time_stamps as an index of interval by dividing each timestamps by 300 000 000
            task_pairs = task_pairs.map(lambda x: (int((x[0] - 6*10**8) / (5*3*10**8)), x[1]))

            acc_tasks = acc_tasks.union(task_pairs)

            # if (i + 2) % 100 == 0:
            #     acc_tasks = acc_tasks.distinct().collect()

            #     acc_tasks = self.sc.parallelize(acc_tasks)

            # If Java heap error: do a collect between files(~100 files)

        # Counting the number of evicted tasks for each interval
        evict_ev_per_interval =  self.sc.parallelize(list(acc_tasks.countByKey().items())).collect()

        print(evict_ev_per_interval[:10])


        acc_task_usage = self.sc.parallelize([])
        for i in range(-1, int(NUM_FILES)):
            # Generate the next file_name to be processed
            file_name = self.utils.get_next_file(i, 3)
            print('Processing file: {}'.format(file_name))

            task_usage_RDD = self.read_file(file_name)

            # From the task_usage RDD, create pairs of time_start and  assigned_memory for each entry
            task_usage_pairs = task_usage_RDD.map(lambda x: (int(x[task_usage_f['time_start']]), float(x[task_usage_f['assigned_RAM']])))

            # Calculating the time_start as an index of interval by dividing each time_start by 300 000 000
            task_usage_pairs = task_usage_pairs.map(lambda x: (int((x[0] - 6*10**8) / (5*3*10**8)), x[1]))

            # count_keys = self.sc.parallelize(list(task_usage_pairs.countByKey().items()))
    
            # Counting the quantity of assigned_memory for each interval in the same file
            mem_sums_per_interval = task_usage_pairs.reduceByKey(lambda x,y: x + y)

            acc_task_usage = acc_task_usage.union(mem_sums_per_interval)

        # Counting the quantity of assigned_memory for each interval between all the usage file
        mem_sums_per_interval = acc_task_usage.reduceByKey(lambda x,y: x + y)
        # Sorting the pairs based on the interval index
        mem_sums_per_interval = mem_sums_per_interval.sortByKey().collect()

        print(mem_sums_per_interval[:10])

        end = time.time()

        self.utils.dump_in_file("Time: {}".format(end-start), self.nb_q)
        # self.utils.dump_in_file(mem_sums_per_interval, self.nb_q)
        self.utils.save_object(mem_sums_per_interval, 9, 'ram_per_time_interv')
        self.utils.save_object(evict_ev_per_interval, 9, 'evict_per_time_interv')

    def question10(self):
        """
        What is the distribution of the machines according to their RAM capacity?
        
        Refer at this as Q1.B in the documentation.
        """
        print('\nQuestion 10')
        self.nb_q = 10

        machine_ev = self.read_file(machine_events_file)
        start = time.time()
        # Extract all unique entries by machine_ID from machine_events
        uniques_machines = machine_ev.map(lambda x: (x[machine_ev_f['Memory']], x[machine_ev_f['machine_ID']])).distinct()

        # Count how many machines exists for each type of 'Memory'
        distribution = uniques_machines.map(lambda x: (x[0], 1)).reduceByKey(add).collect()

        end = time.time()

        self.utils.dump_in_file(distribution, self.nb_q)
        self.utils.dump_in_file("Time: {}".format(end-start), self.nb_q)


    def question11(self):
        """
        What is the distribution of machines as a function of CPU and RAM capacity?
        
        Refer at this as Q1.C in the documentation.
        """
        print('\nQuestion 11')
        self.nb_q = 11

        machine_ev = self.read_file(machine_events_file)
        start = time.time()
        # Extract all unique entries by machine_ID from machine_events
        uniques_machines = machine_ev.map(lambda x: ((x[machine_ev_f['CPUs']], x[machine_ev_f['Memory']]), x[machine_ev_f['machine_ID']])).distinct()

        # Count how many machines exists for each type of 'Memory'
        distribution = uniques_machines.map(lambda x: (x[0], 1)).reduceByKey(add).collect()

        end = time.time()

        self.utils.dump_in_file(distribution, self.nb_q)
        self.utils.dump_in_file("Time: {}".format(end-start), self.nb_q)
        self.utils.save_object(distribution, 11, 'cpu_ram_distribution')
