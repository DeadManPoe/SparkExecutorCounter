
import json
import csv
import os
import sys

class Extractor:
    def __init__(self,filename,targetDirectory):
        self.filename = filename
        self.file = None
        self.targetDirectory = targetDirectory

    def run(self):
        self.openFile()
        self.writeFile()

    def openFile(self):
        if os.path.exists(self.filename):
            try:
                self.file = open(filename)
            except:
                print("Reading error")
                exit(-1)
        else:
            print("The inserted file does not exists")
            exit(-1)

    def writeFile(self):
        headers = ["Application","Executors"]
        targetRow = [self.filename, str(self.extract())]
        writer = None
        f = None
        if !os.path.exists(self.targetDirectory+"/ExecutorCount.csv"):
            f = open(self.targetDirectory"/ExecutorCount.csv","w")
            writer = csv.writer(f, delimiter=',', lineterminator='\n')
            writer.writerow(headers)
        else:
            f = open(self.targetDirectory"/ExecutorCount.csv","a")
            writer = csv.writer(f, delimiter=',', lineterminator='\n')

        writer.writerow(targetRow)
        f.close()


    def extract(self):
        counter = 0
        for line in self.file:
            try:
                data = json.loads(line)
                event = data["Event"]
                if event == "SparkListenerExecutorAdded":
                    counter++
            except:
                print("Json parsing error")
                exit(-1)
        self.file.close()
        return counter;


def main():
    args = sys.argv
    if len(args) != 3:
        print("Required args: [LOG_FILE] [TARGET_DIRECTORY_FOR_RESULTS]")
        exit(-1)
    else:
        extractor = Extractor(str(args[1]),str(args[2]))
        extractor.run()

if __name__ == "__main__":
    main()
