import re
import sys
from heapq import *
from collections import OrderedDict


class Recorder:
    def __init__(self):
        self.heaps = [], []
        self.count = 0
        self.sum = 0

    def add_num(self, num):
        """
        take in a new number and update count, and sum, and running median
        :param num: number that streamed in
        :return: None
        """
        self.count += 1
        self.sum += num
        small, large = self.heaps
        heappush(small, -heappushpop(large, num))
        if len(large) < len(small):
            heappush(large, -heappop(small))

    def find_median(self):
        """
        if two heap size different, pop from larger heap.
        if two heap size same, pop from both heaps and take the mean,
        :return:
        """
        small, large = self.heaps
        if len(large) > len(small):
            return large[0]
        return int(round((large[0] - small[0]) / 2.0))


class Parser:
    def __init__(self, type):
        # Two different output files requires two types of parser
        self.type = type

    @staticmethod
    def is_valid_date(date):
        if re.match(r'^[0-9]{8}$', date):
            return True
        else:
            return

    @staticmethod
    def is_valid_zip(zip):
        if len(zip) >= 5:
            return True
        else:
            return

    def parse(self, line):
        """
        filter out some of features
        :param line: raw line from file
        :return: cmte_id,zip_code,transaction_dt,transaction_amt,other_id
        """
        values = line.strip().split('|')

        # check other_id
        other_id = values[15]
        if other_id:
            return
        else:
            other_id = 'empty'

        # check cmte_id
        cmte_id = values[0]
        if not cmte_id:
            return

        # check transaction_dt
        transaction_dt = values[13]
        if self.type == 'date' and not Parser.is_valid_date(transaction_dt):
            return

        # check zip_code and only consider first 5 digits of zip code
        zip_code = values[10]
        if self.type == 'zip' and not Parser.is_valid_zip(zip_code):
            return
        zip_code = zip_code[:5]

        # check transaction_amt
        transaction_amt = values[14]
        if not transaction_amt:
            return

        return cmte_id, zip_code, transaction_dt, int(transaction_amt), other_id


def process_zip(line, mapper):
    """
    take in new data and calculate the values
    :param line: tuple (cmte_id, zip_code, transaction_dt, transaction_amt, other_id)
    :param mapper: a dictionary caches all data for each 'recipient + zipcode' index, value should be a Recorder instance
    :return: processed string to be written to output files
    """
    parsed = Parser('zip').parse(line)
    if parsed:
        cmte_id, zip_code, transaction_dt, transaction_amt, other_id = parsed
        if mapper.get(cmte_id + zip_code, 0):
            recorder = mapper[cmte_id + zip_code]
        else:
            mapper[cmte_id + zip_code] = Recorder()
            recorder = mapper[cmte_id + zip_code]

        recorder.add_num(transaction_amt)
        running_median = recorder.find_median()
        count = recorder.count
        total_amt = recorder.sum
        mapper[cmte_id + zip_code] = recorder

        return '{}|{}|{}|{}|{}\n'.format(cmte_id, zip_code, running_median, count, total_amt)


def process_date(line, mapper):
    """
    take in new data and calculate the values
    :param line: tuple (cmte_id, zip_code, transaction_dt, transaction_amt, other_id)
    :param mapper: a dictionary caches all data for each 'recipient + date' index, value should be a Recorder instance
    """
    parsed = Parser('date').parse(line)
    if parsed:
        cmte_id, zip_code, transaction_dt, transaction_amt, other_id = parsed
        if mapper.get(cmte_id + '|' + transaction_dt, 0):
            recorder = mapper[cmte_id + '|' + transaction_dt]
        else:
            mapper[cmte_id + '|' + transaction_dt] = Recorder()
            recorder = mapper[cmte_id + '|' + transaction_dt]
        recorder.add_num(transaction_amt)


if __name__ == '__main__':
    argvs = sys.argv
    input_file = argvs[1]
    output_file_zip = argvs[2]
    output_file_date = argvs[3]

    with open(input_file, 'r') as f_in, \
            open(output_file_zip, 'w') as f_out_zip, \
            open(output_file_date, 'w') as f_out_date:
        mapper_zip = {}
        mapper_date = OrderedDict()

        for line in f_in.readlines():
            # write to medianvals_by_zip.txt
            rslt_line_zip = process_zip(line, mapper_zip)
            if rslt_line_zip:
                f_out_zip.write(rslt_line_zip)

            # just update recorder instance
            process_date(line, mapper_date)

        # write to medianvals_by_date.txt
        for k, v in mapper_date.items():
            cmte_id = k.split('|')[0]
            transaction_dt = k.split('|')[1]
            running_median = v.find_median()
            count = v.count
            total_amt = v.sum
            f_out_date.write('{}|{}|{}|{}|{}\n'.format(cmte_id, transaction_dt, running_median, count, total_amt))

