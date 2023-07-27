import random
import string
import time


class Example(object):
    def read(self):
        time.sleep(1)
        records = [''.join(random.choice(string.ascii_letters) for _ in range(8))]
        print(f'read {records}')
        return records

    def set_commit_callback(self, cb):
        pass

    def process(self, records):
        print(f'process {records}')
        return [(record, [record]) for record in records]

    def write(self, records):
        print(f'write {records}')
