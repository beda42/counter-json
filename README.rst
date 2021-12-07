===============================================
COUNTER JSON size optimization - test converter
===============================================

This project contains a simple (and largely incomplete - see `Limitations`_) converter which
implements a few optimizations to the COUNTER JSON format.

It reads COUNTER 5 TR master reports,

Usage
=====

Most basic usage:

>>> python reader.py report.json




Limitations
===========

* only works with TR master report. These reports tend to be the largest ones, so they are the
  most interesting target for optimization.

* expects all details, such as `YOP`, `Section_Type`, `Access_Method`, etc. to be present in the
  data. If they are not present, ``null`` values will be substituted instead, which may increase
  the size of the output files.


Internals
=========

Internally the program parses the incoming JSON into a tabular form stored in a Pandas DataFrame.
This representation is then used to prepare the data in the desired output format. It does not
mainly by applying ``groupby`` in different ways ;).
