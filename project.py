import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from interface import execute_conversion

def main():
    print("Starting SQL to Pipe-Syntax Converter")
    execute_conversion()

if __name__ == "__main__":
    main()