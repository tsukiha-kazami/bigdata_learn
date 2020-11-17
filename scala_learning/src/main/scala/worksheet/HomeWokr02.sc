var list = Array("hello flink", "hello spark", "hello spark")

list.flatMap(_.split(" +"))
  .groupBy(x => x)
  .map(x => (x._1, x._2.size))


