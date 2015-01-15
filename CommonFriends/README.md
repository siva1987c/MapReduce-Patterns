# Problem
find common friends

# Example
input (line 1 means user 100 has friends 200, 300, 400, 500 and 600)

```
100, 200 300 400 500 600
200, 100 300 400
300, 100 200 400 500
400, 100 200 300
500, 100 300
600, 100
```

output 
```
(100, 200) -> 	[400, 300]
(100, 300) -> 	[200, 400, 500]
(100, 400) -> 	[200, 300]
(100, 500) -> 	[300]
(100, 600) -> 	[]
(200, 300) -> 	[100, 400]
(200, 400) -> 	[100, 300]
(300, 400) -> 	[200, 100]
(300, 500) -> 	[100]
```

# Solution
for input 
```
200, 100 300 400
```
output
```
[200, 100] [200, 100, 300 400]
[200, 300] [200, 100, 300 400]
[200, 400] [200, 100, 300 400]
```

for input 
```
400, 100 200 300
```
output
```
[400 100] [400 100 200 300]
[400 200] [400 100 200 300]
[400 300] [400 100 200 300]
```

calculate insect of pair [200, 400] and get [200, 400] have common friends [100, 300]

# How To Run
```
hadoop jar ./target/CommonFriends-1.0-SNAPSHOT.jar com.xboxng.cf.CFDriver sample output
```
