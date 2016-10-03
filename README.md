This repo is an example of how to count the unique words in a text document using Spark and Java 8.

This example is different than most spark+java examples because it uses exclusively lambdas for specifying reduce and map functions in spark which makes the code much more concise.

The `run` method of [Main.java](src/main/java/com/joshterrell/Main.java) contains almost all of the spark code (the `JavaSparkContext` is created in [Beans.xml](src/main/resources/Beans.xml)).

## Excution
To build this example, use maven in your IDE or on the command line.

To run, make sure to supply the path to the file you want to count words from. The `--limit` parameter is optional and allows the you to specify how many of the most frequent words should be printed.

## License
MIT
