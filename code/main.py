from analysis import Analyzer
import analysis
import sys

if __name__ == '__main__':
        
    analysis.MEMORY= sys.argv[2]
    analysis.CORE= sys.argv[3]
    analysis.NUM_FILES= sys.argv[4]
    analysis.PERCENTAGE= sys.argv[5]

    
    analyzer = Analyzer(sys.argv[3])
    getattr(analyzer,'question'+sys.argv[1])()
