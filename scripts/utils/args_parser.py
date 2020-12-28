import argparse

class input:
    def __init__(self, config_file_location):
        self.config_file_location = config_file_location
    
def parse_args():
    parser = argparse.ArgumentParser(description='Pyspark ETL')
    parser.add_argument('-c', '--config', help='Config file location', required=True)
    args = parser.parse_args()
    return args

def get_args():
    args = parse_args()
    config = input(args.config)
    return config