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
