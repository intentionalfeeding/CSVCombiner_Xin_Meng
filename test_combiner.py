import pandas as pd
import os
import sys
import unittest
from io import StringIO
from combiner import Combiner


class TestCombiner(unittest.TestCase):

    test_output_path = "./test_output.csv"
    combiner_path = "./combiner.py"
    csvpath1 = "./accessories.csv"
    csvpath2 = "./clothing.csv"
    csvpath3 = "./household_cleaners.csv"

    # initialize
    backup = sys.stdout
    test_output = open(test_output_path, 'w+')
    combiner = Combiner()

    @classmethod
    def setUpClass(cls):
        sys.stdout = cls.test_output

    @classmethod
    def tearDownClass(cls):

        cls.test_output.close()

        if os.path.exists(cls.csvpath1):
            os.remove(cls.csvpath1)

        if os.path.exists(cls.csvpath2):
            os.remove(cls.csvpath2)

        if os.path.exists(cls.csvpath3):
            os.remove(cls.csvpath3)
            
        if os.path.exists(cls.test_output_path):
            os.remove(cls.test_output_path)

    def setUp(self):
        # setup
        self.output = StringIO()
        sys.stdout = self.output
        self.test_output = open(self.test_output_path, 'w+')

    def tearDown(self):
        self.test_output.close()
        self.test_output = open(self.test_output_path, 'w+')
        sys.stdout = self.backup
        self.test_output.truncate(0)
        self.test_output.write(self.output.getvalue())
        self.test_output.close()

    def test_Nonexisting_file(self):
        argv = [self.combiner_path, 'abc.csv']

        self.combiner.combineCSV(argv)
        self.assertTrue("No such file exists" in self.output.getvalue())
    
    def test_Wrong_suffix(self):
        argv = [self.combiner_path, self.combiner_path]

        self.combiner.combineCSV(argv)
        self.assertTrue("Wrong file format" in self.output.getvalue())

    def test_filename(self):
        
        argv = [self.combiner_path, self.csvpath1, self.csvpath2]
        self.combiner.combineCSV(argv)

        # update the test_output.csv file
        self.test_output.write(self.output.getvalue())
        self.test_output.close()

        # check if filename is added
        
        df = pd.read_csv(self.test_output_path)
        self.assertIn('accessories.csv', df['filename'].tolist())

    def test_size(self):

        argv = [self.combiner_path, self.csvpath1, self.csvpath2]
        self.combiner.combine_files(argv)

        self.test_output.write(self.output.getvalue())
        self.test_output.close()

        df1 = pd.read_csv(self.csvpath1)
        df2 = pd.read_csv(self.csvpath2)

        df = pd.read_csv(self.test_output_path)

        self.assertEqual(df.shape[0], df1.shape[0] + df2.shape[0] - 1)

        
if __name__ == '__main__':
    unittest.main()