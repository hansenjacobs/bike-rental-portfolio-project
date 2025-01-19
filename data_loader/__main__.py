import argparse
from .main import main

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Validate and load citi bike trip and weather data.')
    parser.add_argument('-t', '--type', help='Type of data in specified file.', type=str, required=True, choices=['trips', 'weather'])
    parser.add_argument('-f', '--file', help='Path to file containing data to load', type=str, required=True)

    args = parser.parse_args()

    main(args.file, args.type)