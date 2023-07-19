import random
import string
import time


class Example(object):
    def read(self):
        time.sleep(1)
        records = [''.join(random.choice(string.ascii_letters) for _ in range(8))]
        print('read ' + str(records))
        return records

    def process(self, records):
        print('process ' + str(records))
        return records

    def write(self, records):
        print('write ' + str(records))