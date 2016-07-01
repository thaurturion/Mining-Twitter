import os
import csv
import re


def read_f(src_file):
    tweets = []
    csv.register_dialect('mydialect',delimiter = ',',quotechar = '"',doublequote = True,skipinitialspace = True,lineterminator = '\n',quoting = csv.QUOTE_MINIMAL)
    with open(src_file, mode='r', newline='', encoding="utf-8") as f:
        reader = csv.reader(f, dialect='mydialect')
        tweets = list(reader)
    return tweets
def main():
    
    root = "/Users/fabian/Dropbox/Warwick/Dissertation/Code/UK-files-just-exact/"
    csv.register_dialect('mydialect',delimiter = ',',quotechar = '"',doublequote = True,skipinitialspace = True,lineterminator = '\n',quoting = csv.QUOTE_MINIMAL)
    
    master = []
    for path, subdirs, files in os.walk(root):
        for name in files:
            src_file = os.path.join(path, name)
            print(src_file)
            if src_file[-9:] != ".DS_Store":
                tweets = read_f(src_file)
                t2 = [x for x in tweets if x!= []]
                tweetsFile = open("/Users/fabian/Dropbox/Warwick/Dissertation/Code/UK-files-just-exact/master.csv", 'a', encoding="utf-8", newline='')
                twr = csv.writer(tweetsFile, "mydialect")
                for row in t2:
                    if len(row) > 4:
                        twr.writerow(row) 
            os.remove(src_file)

if __name__ == "__main__":
    main()