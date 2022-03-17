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
#   name: SideInput
#   description: Task from katas to enrich each Person with the country based on the city he/she lives in.
#   multifile: false
#   context_line: 38
#   categories:
#     - Side Input

import apache_beam as beam

from log_elements import LogElements
from typing import Dict

class Person:
    def __init__(self, name, city, country=''):
        self.name = name
        self.city = city
        self.country = country

    def __str__(self):
        return 'Person[' + self.name + ',' + self.city + ',' + self.country + ']'


class EnrichCountryDoFn(beam.DoFn):

    def process(self, element: Person, cities: Dict, *args, **kwargs):
        element.country = cities[element.city]
        yield element


with beam.Pipeline() as p:
    p: beam.Pipeline

    cities_to_countries = {
      'Beijing': 'China',
      'London': 'United Kingdom',
      'San Francisco': 'United States',
      'Singapore': 'Singapore',
      'Sydney': 'Australia'
    }

    persons = [
      Person('Henry', 'Singapore'),
      Person('Jane', 'San Francisco'),
      Person('Lee', 'Beijing'),
      Person('John', 'Sydney'),
      Person('Alfred', 'London')
    ]

    people: beam.PCollection[Person] = p | beam.Create(persons)
    enrich_people: beam.PCollection[Person] = people | beam.ParDo(EnrichCountryDoFn(), cities=cities_to_countries)
    (enrich_people| LogElements())

