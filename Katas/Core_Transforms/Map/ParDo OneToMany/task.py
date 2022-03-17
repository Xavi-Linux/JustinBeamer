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
#   name: MapParDoOneToMany
#   description: Task from katas is a ParDo that maps each input sentence into
#     words splitter by whitespace (" ").
#   multifile: false
#   context_line: 31
#   categories:
#     - Core Transforms

import apache_beam as beam
from log_elements import LogElements

from typing import Iterable, List


class BreakIntoWordsDoFn(beam.DoFn):

    def process(self, element: str, *args, **kwargs) -> Iterable:
        yield element.split(' ')


with beam.Pipeline() as p:
    p: beam.Pipeline

    sentences: beam.PCollection[str] = p | beam.Create(['Hello Beam', 'It is awesome'])
    tokens: beam.PCollection[List[str]] = sentences | beam.ParDo(BreakIntoWordsDoFn())
    flattened_tokens: beam.PCollection[str] = tokens | beam.FlatMap(lambda a: a)
    (flattened_tokens| LogElements())