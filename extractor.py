import json
import csv
import os
import sys

import re


class Extractor:
    def __init__(self, top_directory, target_directory):
        self.top_directory = top_directory
        self.csv_file = None
        self.csv_writer = None
        self.targetDirectory = target_directory

    def run(self):
        self.create_csv()
        pattern = re.compile("application_([0-9]+)_([0-9]+)")
        for f in os.listdir(self.top_directory):
            if pattern.match(f) and not (f.endswith(".zip")):
                self.writefile(f, self.extract(self.top_directory + "/" + f))
        self.csv_file.close()

    def writefile(self, filename, counter):
        target_row = [filename, str(counter)]
        self.csv_writer.writerow(target_row)

    def extract(self, file_):
        f = open(file_, "r")
        counter = 0
        for line in f:
            try:
                data = json.loads(line)
                event = data["Event"]
                if event == "SparkListenerExecutorAdded":
                    counter += 1
            except :
                print("Json parsing error")
                exit(-1)
        f.close()
        return counter

    def create_csv(self):
        headers = ["Application", "Executors"]
        self.csv_file = open(self.targetDirectory + "/Executors.csv", "a")
        self.csv_writer = csv.writer(self.csv_file, delimiter=',', lineterminator='\n')
        self.csv_writer.writerow(headers)


def main():
    args = sys.argv
    if len(args) != 3:
        print("Required args: [TOP_DIRECTORY] [TARGET_DIRECTORY_FOR_RESULTS]")
        exit(-1)
    else:
        extractor = Extractor(str(args[1]), str(args[2]))
        extractor.run()


if __name__ == "__main__":
    main()
