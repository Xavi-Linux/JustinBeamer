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
#   name: WordCount
#   description: Task from katas to create a pipeline that counts the number of words.
#   multifile: false
#   context_line: 29
#   categories:
#     - Combiners

import apache_beam as beam
from typing import Tuple
from log_elements import LogElements

lines = [
    "apple orange grape banana apple banana",
    "banana orange banana papaya"
]

with beam.Pipeline() as p:
    p: beam.Pipeline
    lines: beam.PCollection[str] = p | beam.Create(lines)
    words: beam.PCollection[str] = lines | beam.FlatMap(lambda l: l.split(' '))
    word_count: beam.PCollection[Tuple[str, int]] = words | beam.Map(lambda w: (w, 1))
    group_count: beam.PCollection[Tuple[str, int]] = word_count | beam.CombinePerKey(sum)
    summary: beam.PCollection[str] = group_count | beam.MapTuple(lambda w, c: '{0}:{1}'.format(w,c))
    (summary | LogElements())

