from find_political_donors import *
import os


def exist_test():
    assert os.path.exists('./input'), "input folder doesn't exit"
    assert os.path.exists('./output'), "output folder doesn't exit"
    assert os.path.exists('./input/itcont.txt'), "input file itcont.txt doesn't exit"


def recorder_test():
    ############################ CASE1 ####################################
    # given an input stream [1,2,3,4,5], calculate the count, total, and running median
    recorder = Recorder()
    for i in [1, 2, 3, 4, 5]:
        recorder.add_num(i)
    assert recorder.count == 5 and recorder.sum == 15 and recorder.find_median() == 3, "Recorder class doesn't work as expected"
    ############################ CASE2 ####################################
    # given an input stream [2,3,4,5,6,7], calculate the count, total, and running median
    recorder = Recorder()
    for i in [2, 3, 4, 5, 6, 7]:
        recorder.add_num(i)
    assert recorder.count == 6 and recorder.sum == 27 and recorder.find_median() == 5, "Recorder class doesn't work as expected"


def parser_test():
    ############################ CASE1 ####################################
    line = "C00177436|N|M2|P|201702039042410894|15|IND|DEEHAN, WILLIAM N|ALPHARETTA|GA|300047357|UNUM|SVP, SALES, CL|01312017|384||PR2283873845050|1147350||P/R DEDUCTION ($192.00 BI-WEEKLY)|4020820171370029337"
    result = Parser('zip').parse(line)
    assert result == ('C00177436', '30004', '01312017', 384, 'empty'), "Parser class doesn't work as expected"
    ############################ CASE2 ####################################
    # when parsing for the date case, line without date info should return None
    line = "C00177436|N|M2|P|201702039042410894|15|IND|DEEHAN, WILLIAM N|ALPHARETTA|GA|300047357|UNUM|SVP, SALES, CL||384||PR2283873845050|1147350||P/R DEDUCTION ($192.00 BI-WEEKLY)|4020820171370029337"
    result = Parser('date').parse(line)
    assert not result, "Parser class doesn't work as expected"


def e2e_test():
    # already done by the given insight_testsuite
    pass


if __name__ == '__main__':
    exist_test()
    recorder_test()
    parser_test()
    print("Test passed!")
