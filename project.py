import sys
import os

#force adding project path to python
sys.path.append(os.path.dirname(os.path.abspath(__file__)))


from preprocessing import get_query_plan
from pipesyntax import convert_to_pipe_syntax
from interface import execute_conversion

def main():
    #run the SQL to Pipe-Syntax conversion project
    print("Starting SQL to Pipe-Syntax Conversion Tool...")
    execute_conversion()

if __name__ == "__main__":
    main()