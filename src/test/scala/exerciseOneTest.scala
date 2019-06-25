class exerciseOneTest extends FunSuite with SharedSparkContext {
  test("really simple transformation") {
    val input = List("hi", "hi cloudera", "bye")
    val expected = List(List("hi"), List("hi", "cloudera"), List("bye"))
    assert(SampleRDD.tokenize(sc.parallelize(input)).collect().toList === expected)
  }
}