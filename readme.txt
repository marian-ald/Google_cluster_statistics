/*
 * Authors: Mahdis Ghaffarian
 *			Marian Aldescu
 *
 * Project:Google cluster data analysis with Spark
 */

The archive contains:
- the code files
	* main.py: entry point of the program bun running a certain question
	* analysis.py: the implementation of each question using pyspark
	* utils.py: file with helpers function(save data & output on disk, plot the results)
- the report: report.pdf


Implementation using python3.7 and the following libraries:
- spark-3.0.1
- matplotlib-3.1.0
- numpy-1.16.4
- pickle


How to run the code:
	python3 main.py question_nb memory_in_MB nb_of_cores nb_of_files percentage

	'percentage' argument indicates the number of the files after the triggering the collect()
	If it's too big, we can have memory problems. Usually we used 50,100 for this.

e.g.:
	python3 main.py 5 32768 4 499 100



The directories should have the following structure:

├──	code
│   ├── analysis.py
│   ├── main.py
│   ├── utils.py
├── data
│   ├── job_events
│   │   ├── part-00000-of-00500.csv
	│	├── ...
	├── task_events
	│  	...
	├── task_usage
		...
