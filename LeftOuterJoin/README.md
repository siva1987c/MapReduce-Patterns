# Example
file 1 (user id and user address)
```
u1, ut
u2, ga
u3, ca
u4, ca
u5, ga
```

file 2 (transaction id, production id, user id etc)
```
t1, p3, u1, 3, 330
t2, p1, u2, 1, 400
t3, p1, u1, 3, 600
t4, p2, u2, 10, 1000
t5, p4, u4, 9, 90
t6, p1, u1, 4, 120
t7, p4, u1, 8, 160
t8, p4, u5, 2, 40
```

goal is to find how many distinct addresses a product are sent to. The result for file 1 and file 2 should be:
```
p1	2
p2	1
p3	1
p4	3
```

# Solution
1. Phase 1
Use two mapper. map1 process user file, map2 process transaction file. Map1 emits "(user, "A"), address",  map2 "(user, "T"), transaction". Then, we use "secondary sort" to group address and transaction belonging to a user, making address the first value of the list passed to the reducer. The result of first reducer will be 
```
p3	ut
p1	ut
p1	ut
p4	ut
p1	ga
p2	ga
p4	ca
p4	ga
```
2. Phase 2
count distinct addresses of each product.

# Takeaways
1. It is possible to use multiple mappers

# How to run
```
hadoop jar ./target/LeftOuterJoin-1.0-SNAPSHOT.jar com.xboxng.phase1.Phase1Driver users transactions  output1
hadoop jar ./target/LeftOuterJoin-1.0-SNAPSHOT.jar com.xboxng.phase2.Phase2Driver output1/part-r-00000 output2
```



