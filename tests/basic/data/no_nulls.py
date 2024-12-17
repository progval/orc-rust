# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""
Generates a file with a 'present' stream for each column, but no actual nulls in it
"""

from pathlib import Path

import pyorc

path = Path(__file__).parent / "no_nulls.orc"

with path.open("wb") as data:
    #with pyorc.Writer(data, "struct<col0:int,col1:string>") as writer:
    with pyorc.Writer(data, pyorc.Struct(col0=pyorc.Int(), col1=pyorc.String())) as writer:
        writer.write((1, "row 1"))
        writer.write((2, "row 2"))
