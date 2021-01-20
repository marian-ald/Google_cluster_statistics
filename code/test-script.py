from utils import *
import os

utils = Utils()
utils.format_output_files()
os.system('python main.py 1 32768 4 0 0')
os.system('python main.py 2 32768 4 0 0')
os.system('python main.py 3 32768 4 0 0')
os.system('python main.py 4 32768 4 0 0')
os.system('python main.py 5 32768 4 499 100')
os.system('python main.py 6 32768 4 499 100')
os.system('python main.py 7 32768 4 49 100')
os.system('python main.py 8 32768 4 49 100')
os.system('python main.py 9 32768 4 49 100')

os.system('python main.py 1 32768 8 0 0')
os.system('python main.py 2 32768 8 0 0')
os.system('python main.py 3 32768 8 0 0')
os.system('python main.py 4 32768 8 0 0')
os.system('python main.py 5 32768 8 499 100')
os.system('python main.py 6 32768 8 499 100')
os.system('python main.py 7 32768 8 49 100')
os.system('python main.py 8 32768 8 49 100')
os.system('python main.py 9 32768 8 49 100')