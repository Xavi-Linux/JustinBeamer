#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

# beam-playground:
#   name: SideOutput
#   description: Task from katas to implement additional output to your ParDo for numbers bigger than 100.
#   multifile: false
#   context_line: 31
#   categories:
#     - Filtering
#     - Multiple Outputs

import apache_beam as beam
from apache_beam import pvalue

from log_elements import LogElements
from typing import Tuple

num_below_100_tag = 'num_below_100'
num_above_100_tag = 'num_above_100'


class ProcessNumbersDoFn(beam.DoFn):

    def process(self, element: int, *args, **kwargs) -> Tuple[str, int]:
        if element <= 100:
            yield pvalue.TaggedOutput(num_below_100_tag, element)
        else:
            yield pvalue.TaggedOutput(num_above_100_tag, element)


with beam.Pipeline() as p:
    p: beam.Pipeline

    values: beam.PCollection[int] = p | beam.Create([10, 50, 120, 20, 200, 0])
    results: beam.pvalue.DoOutputsTuple = values | beam.ParDo(ProcessNumbersDoFn()).with_outputs(num_below_100_tag, num_above_100_tag)
    results[num_below_100_tag] | 'Log numbers <= 100' >> LogElements(prefix='Number <= 100: ')
    results[num_above_100_tag] | 'Log numbers > 100' >> LogElements(prefix='Number > 100: ')

