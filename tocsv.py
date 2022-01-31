# coding=utf8
# the above tag defines encoding for this document and is for Python 2.x compatibility

import re

regex = r"^([0-9]*)-([0-9]*)-([0-9]*).log:.*- 97 - (.*?):(.*?) .*?([0-9.]+) .*?([0-9.]+) .*?([0-9.]+) .*?([0-9.]+) .*?([0-9.]+)"




# You can manually specify the number of replacements by changing the 4th argument

with open("a.txt") as f:
    for l in f:
        result = re.match(regex,l)
        if result:
            print(','.join(result.groups()))
        else:
            print("ERROR")


