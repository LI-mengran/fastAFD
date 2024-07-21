## FastRFD-Exp

### Datasets

Provided in `datasets.zip`, and the corresponding similarity threshold are provided in `threshold.zip`.

### Exp-1 : FastRFD against baseline methods

We compare the runtime of different algorithms `FastRFD`、`ColRFD` and `RowRFD`, which are of `mode[1]`、`mode[7]` and `mode[8]`. You can refer to the following code to run them.

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

We study the scalability in terms of `|𝑟|`, `|P|` and `|e|`. 

- Varying `|𝑟|`:

  We conduct experiments on `FastRFD`、`ColRFD` and `RowRFD` to find scalability in terms of `|𝑟|`.Option `[-r]` is used to specialize the row of input file to deal with.

- Varying `|P|`:

  We conduct experiments on `FastRFD`、`ColRFD` and `RowRFD` to find scalability in terms of `|P|`. The corresponding predicate sets are provided in  threshold.zip.

- Varying `|e|`:

  We conduct experiments on `RFDD`、`RFDD-` and `COlEnum` to find scalability in terms of `|e|`. The latter can be found in `mode[2]` and `mode[4]`.

### Exp-3 : FastRFD against Dime

The source code of Dime can be found [here](https://dastlab.github.io/dime/). Because of the limit of Dime, we specialize similarity threshold to 2 for all attributes, and error threshold to 0.1.

### Exp-4 : FastRFD against Domino

The source code of Domino can be found [here](https://dast-unisa.github.io/Domino-SW/). After generating predicate sets from Domino, we use them in FastRFD, and set error threshold to 0. Here is an example:

```
java -jar FastAFD.jar -f ./dataset/iris.csv -t 0.0 -g -s threshold/iris.txt -m 1
```

### Exp-5 : Top-k discovery

We set k = 20 to find the meaningful RFDs and manually judge the meaningfulness of them. The detail information is put in the `"Exp-5"` directory.

### Exp-6 : Handling dirty data

We compare different methods in their abilities of identifying FDs from dirty data. We first conduct FD discovery on `𝑟` to identify the set `Σ` of minimal and valid FDs as the ground truth, and then inject errors into `𝑟` to generate a dirty dataset `𝑟′`, using different settings for different methods to find RFDs. The dirty datasets and the results are put in the `"Exp-6"` directory.
