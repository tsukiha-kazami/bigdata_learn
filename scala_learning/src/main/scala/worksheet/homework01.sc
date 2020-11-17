var list = for (i <- 1 to 100) yield i

//way 1
var sum = 0
list.foreach(f = x => sum += x)
println("=============way1=============")
println("sum = " + sum)

//way 2
println("=============way2=============")
list.reduce((x, y) => x + y)

//way 3
println("=============way3=============")
list.fold(0)((x, y) => x + y)

//
//class Counter{def counter = "counter"}
//class Counter{val counter = counter""}
//class Counter{var counter:String}
//class Counter{def counter () {}}



list.map(_*1)