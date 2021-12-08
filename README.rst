===============================================
COUNTER JSON size optimization - test converter
===============================================

This project contains a simple (and largely incomplete - see `Limitations`_) converter which
implements a few optimizations to the COUNTER JSON format.

It works with COUNTER 5 TR master reports.

Installation
============

This project uses `poetry <https://python-poetry.org/>`_ for dependency management. Simply doing
``poetry install`` inside the source directory should install all the requirements.

In case you cannot or do not want to install poetry, there is also the ``requirements.txt`` file
which can be used to install the dependencies using pip like this -
``pip install -r requirements.txt``.

Usage
=====

Basic usage:

>>> python converter.py -c simplify_performance sample-input/TR_1mo_tiny.json

The program will process each JSON file given on the command line and print out statistics
about the file size and memory consumption of each file.

The ``-c`` argument selects the converter to use. At present there are two:

``avoid_duplicate_metadata``
    Merges all usage related to one title into one ``Report_Item``. Also simplifies the was
    performance is stored. Converts: ``[{"Metric": "M1", "Count": 5}]`` to ``{"M1": 5}``.
    Sample output is available `here <sample-output/TR_1mo_tiny.avoid_duplicate_metadata.json>`_.

``simplify_performance``
    Does the same as ``avoid_duplicate_metadata``, but also moves date inside the metric record,
    so the result looks like ``{"M1": {"2020-01-01": 5}}`` and the ``Period`` record is removed.
    Sample output is available `here <sample-output/TR_1mo_tiny.simplify_performance.json>`_.

By default the program uses two parallel processes, so it processes two input files in parallel.
If you have more cores and enough memory, you can raise this by using the ``-j`` switch like
``-j8`` or ``-j1`` for single-process operation.

If you would like to see the converted output of the JSON report, you can use the ``-o`` switch.
It will cause the program to print out the new JSON output onto stdout where you can redirect it
into a file. Please note that this output will be indented for better human readability and the
file size will thus differ from that used in the printed statistics. Also note that for multiple
input file all the output will be printed to stdout so it may be hard to untangle individual
outputs. Because of this ``-o`` is mostly useful when one file is processed.


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
This representation is then used to prepare the data in the desired output format. It does so
mainly by applying ``groupby`` in different ways ;).
