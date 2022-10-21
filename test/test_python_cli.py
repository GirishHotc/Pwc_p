#!/usr/bin/env python

import pandas as pd
import unittest
from Sales_data_pipelines import merger_dt

df_1 = pd.DataFrame({'num_a': [1, 1, 1, 1],
                     'num_b': [1, 1, 1, 1],
                     'num_c': [1, 1, 1, 1]})
df_2 = pd.DataFrame({'num_a': [2, 2, 2, 2],
                     'num_b': [2, 2, 2, 2],
                     'num_c': [2, 2, 2, 2]})
df_3 = pd.DataFrame({'num_a': [3, 3, 3, 3],
                     'num_b': ['c', 'c', 'c', 'c'],
                     'num_c': [3, 3, 3, 3]})

class TestPython(unittest.TestCase):

    def test_1_response(self):
        output = merger_dt(df_first=df_1,df_second=df_2,df_third=df_3)
        assert (len(output) == 12)
        assert (output['num_a'].dtypes == 'int64')
        assert (output['num_b'].dtypes == 'object')
        assert (output['num_c'].dtypes == 'int64')

