# FastRFD

### Introduction

RFDs is the first algorithm to  discovering all valid and minimal RFDs with relaxing restrictions in value equality and constraint satisfaction simultaneously.

### Requirements

- Java 11 or  later
- Maven 3.1.0

### Usage

After building the project with maven, you can get `FastRFD.jar`. There are  11 command options you can choose:

```
 -f = < > | String | Default: ""| input file 
 -r = < > | Integer | Default: -1 | row limit ( -1 for all rows )
 -d = < > | Integer | Default 10000 | pliShard ( only for mode 8 )
 -i | Boolean| Default false | output diff-set to file
 -o | Boolean | Default false | output the result
 -g | Boolean | Default true | g1 = 0 (false) 
 -t = < > | Double | Default 0.01 | g1 error
 -s = < > | String | Default "" | similarity threshold file
 -e = < > | String | Default "" | diff-set file (if exists)
 -m = < > | Integer | Default 1 | mode
 -k = < > | Integer | Default 50 | topK number ( only for mode 6)
```

And there are modes you can choose:

```
mode [1] | FastRFD
mode [2] | DiffBuilder + ColEnum
mode [4] | DiffBuilder + RFDD-
mode [6] | Topk
mode [7] | ColRFD
mode [8] | rowRFD
```

When you want to run our code, you should choose options you need. Among these,  `[-f]、[-s]`  are necessary to input.

For example, you can run this code to find RFDs in "iris.csv" with it's given similarity threshold file "threshold/iris.txt" and g1 error 0.1.

```
java -jar FastAFD.jar -f ./dataset/iris.csv -t 0.1 -s threshold/iris.txt
```

### Comparative Experiments

FastRFD are compared to other three discovery methods, `Domino` , `Dime` and `pyro`. The source code of Domino can be found [here](https://dast-unisa.github.io/Domino-SW/). The source code of Domino can be found [here](https://dastlab.github.io/dime/). The source code of Domino can be found [here](https://github.com/HPI-Information-Systems/pyro). Other methods can be found in our codes.

## License

FastRFD is released under the [Apache 2.0 license](https://github.com/RangerShaw/FastADC/blob/master/LICENSE).
