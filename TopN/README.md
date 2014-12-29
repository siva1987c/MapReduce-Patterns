## Problem
1. Find Top N

## Example
word frequeces
```
google,33
gmail,22
groupon,73
facebook,29
linkedin,16
yahoo,84
youtube,84
apple,55
slickdeals,97
fatwallet,10
fidelity,68
microsoft,15
sony,51
ps3,122
xbox,82
linkedin,14
yahoo,81
microsoft,16
sony,55
linkedin,17
google,73
gmail,82
groupon,39
facebook,24
linkedin,71
yahoo,78
microsoft,21
sony,53
linkedin,51
google,30
slickdeals,71
fatwallet,13
fidelity,68
microsoft,14
sony,65
vanguard,99
chschwab,23
fidelity,26
microsoft,15
```
top 10 most frequence word:
```
99	vanguard
104	gmail
112	groupon
122	ps3
136	google
162	fidelity
168	slickdeals
169	linkedin
224	sony
243	yahoo
```

## Solution
1. multiple mappers, single reducer
1. each mapper finds top N, and sends them to reducer
1. reducer finds the top N

## takeaways
1. mapper is stateful because it needs to find its own top N
1. as mapper is stateful, mapper shouldn't send data to reducer in *map* method; *cleanup* method is the right choice;
1. all data send to reducer having the same key (e.g. NullWritable)

## How To Run
```
hadoop jar ./target/TopN-1.0-SNAPSHOT.jar com.xboxng.aggregate.AggregateKeyDriver topn/sample topn/output1
hadoop jar ./target/TopN-1.0-SNAPSHOT.jar com.xboxng.topn.TopNDriver topn/output1/part-r-00000 topn/output2
```
