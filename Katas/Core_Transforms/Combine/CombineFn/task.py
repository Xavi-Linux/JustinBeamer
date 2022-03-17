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
#   name: CombineFn
#   description: Task from katas averaging.
#   multifile: false
#   context_line: 30
#   categories:
#     - Combiners

import apache_beam as beam

from log_elements import LogElements
from typing import Tuple, Union


class AverageFn(beam.CombineFn):

  def create_accumulator(self, *args, **kwargs) -> Tuple[float, int]:
    return 0.0, 0

  def add_input(self, mutable_accumulator: Tuple[float, int], element: int, *args, **kwargs) -> Tuple[float, int]:
    (sums, counts) = mutable_accumulator

    return sums + element, counts + 1

  def merge_accumulators(self, accumulators, *args, **kwargs) -> Tuple[float, int]:
    sums, counts = zip(*accumulators)
    return sum(sums), sum(counts)

  def extract_output(self, accumulator:Tuple[float, int], *args, **kwargs) -> beam.PCollection[float]:
    sums, counts = accumulator
    return sums / counts if counts != 0 else float('NaN')


with beam.Pipeline() as p:
  p: beam.Pipeline

  numbers: beam.PCollection[int] = p | beam.Create([10, 20, 50, 70, 90])
  result: beam.PCollection[float] = numbers | beam.CombineGlobally(AverageFn())
  (result | LogElements())

