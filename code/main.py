from analysis import Analyzer
import analysis
import sys

if __name__ == '__main__':
        
    analysis.MEMORY= sys.argv[2]
    # analysis.EXE_CORE= sys.argv[3]
    # analysis.DV_MEMORY= sys.argv[4]
    # analysis.DV_CORE= sys.argv[5]
    # analysis.DV_MAX= sys.argv[6]

    analysis.NUM_FILES= sys.argv[4]
    analysis.PERCENTAGE= sys.argv[5]

    
    analyzer = Analyzer(sys.argv[3])
    
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