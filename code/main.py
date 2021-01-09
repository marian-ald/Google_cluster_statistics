from analysis import Analyzer
import analysis
import sys

if __name__ == '__main__':
    
    analysis.NUM_FILES = sys.argv[2]
    analysis.MEMORY = sys.argv[3]
    analysis.CORE = sys.argv[4]    
    analyzer = Analyzer(8)
    
    getattr(analyzer,'question'+sys.argv[1])()

#if __name__ == '__main__':
#    analyzer = Analyzer(8)

    # analyzer.question1()

    # analyzer.question2()

    # analyzer.question3()

    # analyzer.question4()

    # analyzer.question5()

    # analyzer.question6()

    # analyzer.question7()

    # analyzer.question8()

    # analyzer.question9()