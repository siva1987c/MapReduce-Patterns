# Problem
Calculate relative frequecies of words for a given set of documents

For example, if input is:

```
java is a great language
java is a programming language
java is green fun language
java is great
programming with java is fun
```

output would be:

```
(a,great)	0.125
(a,is)	0.25
(a,java)	0.25
(a,language)	0.25
(a,programming)	0.125
(fun,green)	0.2
(fun,is)	0.4
(fun,java)	0.2
(fun,language)	0.2
(great,a)	0.2
(great,is)	0.4
(great,java)	0.2
(great,language)	0.2
(green,fun)	0.25
(green,is)	0.25
(green,java)	0.25
(green,language)	0.25
(is,a)	0.14285714285714285
(is,fun)	0.14285714285714285
(is,great)	0.14285714285714285
(is,green)	0.07142857142857142
(is,java)	0.35714285714285715
(is,programming)	0.07142857142857142
(is,with)	0.07142857142857142
(java,a)	0.16666666666666666
(java,fun)	0.08333333333333333
(java,great)	0.08333333333333333
(java,green)	0.08333333333333333
(java,is)	0.4166666666666667
(java,programming)	0.08333333333333333
(java,with)	0.08333333333333333
(language,a)	0.3333333333333333
(language,fun)	0.16666666666666666
(language,great)	0.16666666666666666
(language,green)	0.16666666666666666
(language,programming)	0.16666666666666666
(programming,a)	0.2
(programming,is)	0.2
(programming,java)	0.2
(programming,language)	0.2
(programming,with)	0.2
(with,is)	0.3333333333333333
(with,java)	0.3333333333333333
(with,programming)	0.3333333333333333
```
# How To Run
```
hadoop jar target/OrderInversion-1.0-SNAPSHOT.jar com.xboxng.OrderInversionDriver sample output
```
