#   Licensed to the Apache Software Foundation (ASF) under one
#   or more contributor license agreements.  See the NOTICE file
#   distributed with this work for additional information
#   regarding copyright ownership.  The ASF licenses this file
#   to you under the Apache License, Version 2.0 (the
#   "License"); you may not use this file except in compliance
#   with the License.  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

# beam-playground:
#   name: Partition
#   description: Task from katas that splits a PCollection of numbers into two PCollections. The first PCollection
#     contains numbers greater than 100, and the second PCollection contains the remaining numbers.
#   multifile: false
#   context_line: 31
#   categories:
#     - Multiple Outputs

import apache_beam as beam

from log_elements import LogElements
from typing import List


def partition_fn(number: int, num_partitions: int) -> int:
    return 1 if number <= 100 else 0


with beam.Pipeline() as p:
    p: beam.Pipeline

    values: beam.PCollection[int] = p | beam.Create([1, 2, 3, 4, 5, 100, 110, 150, 250])
    results: List[beam.PCollection[int]] = values | beam.Partition(partition_fn, 2)
    (results[0] | 'Log numbers > 100' >> LogElements(prefix='Number > 100: '))
    (results[1] | 'Log numbers <= 100' >> LogElements(prefix='Number <= 100: '))