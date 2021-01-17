import matplotlib.pyplot as plt
from matplotlib.legend_handler import HandlerPatch
from matplotlib.collections import PatchCollection
import matplotlib.patches as mpatches
import numpy as np
import pickle
import math
import sys


def compare(x, y):
    """
    docstring
    """
    if x == y:
        return x
    elif x[1] == 'E':  
        return x
    elif y[1] == 'E':  
        return y
    else:
        return x

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
        pass

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
        elif file_type == 3:    # task usage type
            return '../data/task_usage/part-00{}-of-00500.csv'.format(file_no)


    def format_output_files(self):
        """
        Delete the content of all the output files.
        """
        for nb_question in range(1,10):
            file_path = '../results/q{}/q{}_output.txt'.format(nb_question, nb_question)
            with open(file_path, 'w') as out_file:
                out_file.write('Question {}\n'.format(nb_question))


    def dump_in_file(self, string, nb_question):
        file_path = '../results/q{}/q{}_output.txt'.format(nb_question, nb_question)
        with open(file_path, 'a') as out_file:
            string = str(string)
            out_file.write(string)
            out_file.write('\n')
            # print(string)


    def save_object(self, obj, nb_question, name):
        file_path = '../results/q{}/q{}_{}.pkl'.format(nb_question, nb_question, name)
        with open(file_path, 'wb') as output:
            pickle.dump(obj, output)


    def load_object(self, nb_question, name):
        file_path = '../results/q{}/q{}_{}.pkl'.format(nb_question, nb_question, name)
        try:
            with open(file_path, 'rb') as pkl_file:
                read_obj = pickle.load(pkl_file)
                return read_obj    
        except IOError:
            print('File {} does not exist.'.format(file_path))
            sys.exit()


    def plot_q4(self):
        """
        docstring
        """
        data = self.load_object(4, 'class_prio')
        # data = [(0, (0, 3)), (1, (1, 4)), (2, (1, 3)), (3, (2, 5))]
        # We are expecting the data to have the following format:
        #       [(job_class, (mean_task_class, mean_task_priority))]
        print(data)
        labels = [x[0] for x in data]
        y_mean_class = [x[1][0] for x in data]
        y_mean_prio = [x[1][1] for x in data]
        
        plot = plt.bar(labels, y_mean_class, width=0.5)
        
        # Add the data value on head of the bar
        for value in plot:
            height = value.get_height()
            plt.text(value.get_x() + value.get_width()/2.,
                    1.002*height,'%.2f' % height, ha='center', va='bottom')
        
        # Add labels and title
        plt.xlabel("Job class")
        plt.ylabel("Tasks class average")
        plt.savefig('../plots/q4/q4_mean_tasks_class.png')        

        # Display the graph on the screen
        # plt.show()
        plt.clf()
        plt.cla()
        plt.close()
        # The second plot
        plot = plt.bar(labels, y_mean_prio, width=0.5)
        # Add the data value on head of the bar
        for value in plot:
            height = value.get_height()
            plt.text(value.get_x() + value.get_width()/2.,
                    1.002*height,'%.2f' % height, ha='center', va='bottom')

        # Add labels and title
        plt.xlabel("Job class")
        plt.ylabel("Tasks priority average")
        plt.savefig('../plots/q4/q4_mean_tasks_prio.png')        


    def plot_q5(self):
        """
        docstring
        """
        data = self.load_object(5, 'evict_probab')
        # data = [(0, 0.09947520490594965), (2, 0.023046092184368736), (9, 0.007751937984496124)]

        labels = [x[0] for x in data]
        y_evict_prob = [x[1] for x in data]
        
        plot = plt.bar(labels, y_evict_prob, width=0.5)
        
        # Add the data value on head of the bar
        for value in plot:
            height = value.get_height()
            plt.text(value.get_x() + value.get_width()/2.,
                    1.002*height,'%.2f' % height, ha='center', va='bottom')
        plt.xticks(np.arange(min(labels), max(labels)+1, 1.0))
        # Add labels and title
        plt.xlabel("Task priority")
        plt.ylabel("Evict probability")
        plt.savefig('../plots/q5/q5_evict_probab.png')        

        # Display the graph on the screen
        plt.show()


    def plot_q6(self):
        """
        docstring
        """
        data = self.load_object(6, 'jobs_per_dif_machines')
        # data = [(0, 0.09947520490594965), (2, 0.023046092184368736), (9, 0.007751937984496124)]
        # We are expecting the data to have the following format:
        #       [(j, (mean_task_class, mean_task_priority))]

        # data.remove((10254, 1))
        # data.remove((4, 851))
        data.sort(key=lambda tup: tup[0])

        # data = data[:15]
        print(data)

        labels = [x[0] for x in data]
        y_evict_prob = [x[1] for x in data]
        
        plt.scatter(labels, y_evict_prob, color='red', s=13, alpha = 0.6)

        # plot = plt.bar(labels, y_evict_prob, width=0.5)
        
        # # Add the data value on head of the bar
        # for value in plot:
        #     height = value.get_height()
        #     plt.text(value.get_x() + value.get_width()/2.,
        #             1.002*height,'', ha='center', va='bottom')
        
        # plt.xticks(np.arange(min(labels), max(labels)+1, 2.0))
        # Add labels and title
        plt.xlabel("Jobs")
        plt.ylabel("Machines")
        plt.savefig('../plots/q6/q6_jobs_per_dif_machines.png')        

        # Display the graph on the screen
        plt.show()
    

    def plot_q7(self):
        """
        docstring
        """
        data = self.load_object(7, 'req_and_used_mem')

        x_axis = [x[0] for x in data]
        y_axis = [x[1] for x in data]
        
        plt.scatter(x_axis, y_axis, color='red', s=3)
        plt.xlabel("Requested RAM")
        plt.ylabel("Used RAM")
        plt.savefig('../plots/q7/q7_req_and_used_mem.png')        

        plt.show()


    def plot_q8(self):
        """
        docstring
        """
        data = self.load_object(8, 'used_ram_per_prio')
        # data = [(0, 0.004012073604694524), (1, 0.009458697604888057), (2, 0.009875916987298474), (8, 0.03020522469193485), (9, 0.049904689365639375), (10, 0.03469358183469384), (11, 0.026471382374860013)]

        data = data[:15]
        print(data)

        labels = [x[0] for x in data]
        y_used_ram = [x[1] for x in data]
        
        plot = plt.bar(labels, y_used_ram, width=0.5)
        
        # Add the data value on the head of the bar
        for value in plot:
            height = value.get_height()
            plt.text(value.get_x() + value.get_width()/2.,
                    1.002*height,'', ha='center', va='bottom')
        
        plt.xticks(np.arange(min(labels), max(labels)+1, 1.0))
        # Add labels and title
        plt.xlabel("Priority")
        plt.ylabel("Used RAM")
        plt.savefig('../plots/q8/q8_used_RAM_per_priority.png')        
        plt.show()


    def plot_q9(self):
        """
        docstring
        """
        data_evict = self.load_object(9, 'evict_per_time_interv')
        data_ram = self.load_object(9, 'ram_per_time_interv')

        # data = [(0, 0.004012073604694524), (1, 0.009458697604888057), (2, 0.009875916987298474), (8, 0.03020522469193485), (9, 0.049904689365639375), (10, 0.03469358183469384), (11, 0.026471382374860013)]

        x_labels = [x[0] for x in data_evict]

        y_evict = [x[1] for x in data_evict]
        y_used_ram = [x[1] for x in data_ram]

        plot = plt.plot(x_labels, y_evict, 'b')
        
        # plt.xticks(np.arange(min(labels), max(labels)+1, 2.0))
        # Add labels and title
        plt.xlabel("Time")
        plt.ylabel("Evict tasks")
        plt.savefig('../plots/q9/q9_evict_tasks.png')        
        plt.show()
        
        # Clear the previous plot from plt
        plt.clf()
        plt.cla()
        plt.close()

        plot = plt.plot(x_labels, y_used_ram, 'r')
        
        # Add labels and title
        plt.xlabel("Time")
        plt.ylabel("RAM used")
        plt.savefig('../plots/q9/q9_ram_used.png')        
        plt.show()


    def plot_q11(self):
        """
        docstring
        """
        data = self.load_object(11, 'cpu_ram_distribution')
        data.remove((('', ''), 32))

        # Cast to float the entries
        data = [(float(x[1]), (round(float(x[0][0]), 2), round(float(x[0][1]),2))) for x in data]
        data.sort(key=lambda tup: tup[0])

        fig, ax = plt.subplots()
        ax.set_xlim(0, 1.1)
        ax.set_ylim(0, 1.1)
    
        # Add circles to the plot
        for c in data:
            plt.scatter(c[1][0], c[1][1], marker = "o", s = c[0], alpha = 0.8)

        # Add labels to axis
        plt.xlabel("CPU capacity")
        plt.ylabel("RAM capacity")
        plt.savefig('../plots/q11/q11_req_and_used_mem.png')        

        plt.show()


# u = Utils()
# u.plot_q7()