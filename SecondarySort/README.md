## Problem
1. Keys are sorted when being sent to Reducer
1. Values of a key is not sorted
1. We want to sort values as well e.g.

## Example
Below are stock prices on different days, we want to sort the data not only by stock symbols, but also dates.
```
ILMN, 2013-12-05, 97.65
GOOG, 2013-12-09, 1078.14
IBM, 2013-12-09, 177.46
ILMN, 2013-12-09, 101.33
ILMN, 2013-12-06, 99.25
GOOG, 2013-12-06, 1069.87
IBM, 2013-12-06, 177.67
GOOG, 2013-12-05, 1057.34
```
i.e. the result should be:
```
GOOG	(2013-12-05, 1057.34)(2013-12-06, 1069.87)(2013-12-09, 1078.14)
IBM	(2013-12-06, 177.67)(2013-12-09, 177.46)
ILMN	(2013-12-05, 97.65)(2013-12-06, 99.25)(2013-12-09, 101.33)
```

## Solution
1. Composite Key
1. Need to program the partitioner so that composite keys can be partitioned properly (NaturalKeyPartitioner)
1. Need to program a grouping comparator so that "similar" composite keys can be grouped (NaturalKeyGroupingComparator)

## How to run
```
hadoop jar target/SecondarySort-1.0-SNAPSHOT.jar com.xboxng.SecondarySortDriver sample output
```
