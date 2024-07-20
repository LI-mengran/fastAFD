
## FastFD-Exp

### Datasets

Provided in datasets.zip, and the corresponding similarity threshold are provided in threshold.zip.

### Exp-1 : FastRFD against baseline methods

We compare the runtime of different algorithms FastRFD、ColRFD and RowRFD, which are of mode[1]、mode[7] and mode[8]. You can refer to the following code to run them.

-  FastRFD:

```
java -jar FastAFD.jar -f ./dataset/iris.csv -t 0.01 -s threshold/iris.txt -m 1
```

- ColRFD:

```
java -jar FastAFD.jar -f ./dataset/iris.csv -t 0.01 -s threshold/iris.txt -m 7
```

- RowRFD ( additional option -d to specialize the size of pli shards) :

```
java -jar FastAFD.jar -f ./dataset/iris.csv -t 0.01 -s threshold/iris.txt -m 8 -d 10000
```

### Exp-2 :  Scalability
