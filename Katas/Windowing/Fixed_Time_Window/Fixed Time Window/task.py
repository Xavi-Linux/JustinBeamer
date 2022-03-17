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
#   name: WindowingFixedTime
#   description: Task from katas to count the number of events that happened based on fixed window with 1-day duration.
#   multifile: false
#   context_line: 35
#   categories:
#     - Windowing
#     - Combiners

from datetime import datetime
import pytz
from typing import Tuple
import apache_beam as beam
from apache_beam.transforms import window

from log_elements import LogElements


with beam.Pipeline() as p:
  p: beam.Pipeline

  events: beam.PCollection[str] = p | beam.Create([
          window.TimestampedValue("event", datetime(2020, 3, 1, 0, 0, 0, 0, tzinfo=pytz.UTC).timestamp()),
          window.TimestampedValue("event", datetime(2020, 3, 1, 0, 0, 0, 0, tzinfo=pytz.UTC).timestamp()),
          window.TimestampedValue("event", datetime(2020, 3, 1, 0, 0, 0, 0, tzinfo=pytz.UTC).timestamp()),
          window.TimestampedValue("event", datetime(2020, 3, 1, 0, 0, 0, 0, tzinfo=pytz.UTC).timestamp()),
          window.TimestampedValue("event", datetime(2020, 3, 5, 0, 0, 0, 0, tzinfo=pytz.UTC).timestamp()),
          window.TimestampedValue("event", datetime(2020, 3, 5, 0, 0, 0, 0, tzinfo=pytz.UTC).timestamp()),
          window.TimestampedValue("event", datetime(2020, 3, 8, 0, 0, 0, 0, tzinfo=pytz.UTC).timestamp()),
          window.TimestampedValue("event", datetime(2020, 3, 8, 0, 0, 0, 0, tzinfo=pytz.UTC).timestamp()),
          window.TimestampedValue("event", datetime(2020, 3, 8, 0, 0, 0, 0, tzinfo=pytz.UTC).timestamp()),
          window.TimestampedValue("event", datetime(2020, 3, 10, 0, 0, 0, 0, tzinfo=pytz.UTC).timestamp()),
       ])
  windowed = events | beam.transforms.WindowInto(window.FixedWindows(60 * 60 * 24))
  grouped: beam.PCollection[Tuple[str, int]] = windowed | beam.combiners.Count().PerElement()
  (grouped | LogElements(with_window=True))

