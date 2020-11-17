
val a = List(1,2)
//a = List(3,4)

//mutable list
//var b = List ()    => b = List


//val a=new Array[Int](10)

// 用元素直接初始化数组
//val/var 变量名 = Array(元素1, 元素2, 元素3...)

//用到了Array类的伴生对象的apply方法，生成Array类的对象
val b =Array("hadoop","spark","hive")

//在scala里边，创建集合时，如果不指定创建的集合时可变或不可变，那么默认创建的就是immutable

val list = List(1,2,3)

import scala.collection.mutable.ArrayBuffer
//val a1 =ArrayBuffer[Int]()
//
//val b = ArrayBuffer("hadoop", "storm", "spark")
//
//a1 += 3
//println(a1)

val a = ArrayBuffer("hadoop", "spark", "flink")
a += "flume"

a -= "hadoop"

a ++= Array("abc", "def")

println(a)

a --= Array("abc", "def")

for(i <- a)println(i)

for(i <- 0 to a.length -1 )println(a(i))


val array=Array(1,3,4,2,5)
array.sorted //生成了一个新的Array
println(array.toBuffer)

val a2 = (1, "张三", 20, "北京市")
a2._1 //第一个元素
a2._2

val a3 = (1, "abc", ("zhangsan", 24))
a3._3._2 //24

//a3._2 = "def" //元组中的元素都是val，不能重新的复制

//默认的不可变的
val map1 = Map("zhangsan"->30, "lisi"->40)

map1.get("zhangsan")

import scala.collection.mutable.Map

val map3 = Map("zhangsan"->30, "lisi"->40)
println(map3.get("wangwu")) //None

map3.getOrElse("wangwu", 50)

map3("lisi")=50

map3 += ("wangwu" ->35)
println(map3)

map3 -= "wangwu"
println(map3)

//析构
for((k,v) <- map3) println(k+" -> "+v)// good

for(kv <- map3) {
  //kv 应该是（key，value）
  println(kv._1 + "--" + kv._2)
}

val  list1=List(1,2,3,4)
//list分头head、尾tail

print(list1.head)
println(list1.tail)

val list2 = Nil
println(list2)

val list3 = 1::2::3::Nil

import scala.collection.mutable.ListBuffer
val list4=ListBuffer(1,2,3,4)

list4(0)
list4.head

list4.tail

list4.+=(5)
list4 += 5

list4 ++= List(6,7)

list4.toList

list4.toArray

val list5=List(1,2,3,4)
//f: Int => Unit
list5.foreach( (x: Int) => {
  println(x)
})

//scala的省略的写法
list5.foreach( x => {
  println(x)
})

list5.foreach( println(_))

array.foreach(println(_))

val list6=List(1,2,3,4)
//f: Int => Int
list6.map((x: Int) => {x * 2})
list6.map(_ * 2)

val list7 = List("hadoop hive spark flink", "hbase spark")
print(list7.map(_.split(" ")).toList)

list7.map(_.split(" ")).flatten

list7.flatMap(_.split(" ")) //List[String] = List(hadoop, hive, spark, flink, hbase, spark)

val list8=List(1,2,3,4,5,6,7,8,9,10)

list8.filter(x => x % 2 == 0)

val list9=List(5,1,2,4,3)
list9.sorted

val list10=List("3 flink", "1 hadoop","2 spark")
list10.sortBy((x: String)/*3 flink*/ => {
  x.split(" ")/*Array("3", "flink")*/(1)
})
list10.sortBy(x => x.split(" ")(0))

//def sortWith(lt: (A, A) ⇒ Boolean): List[A]

val list11 = List(2,3,1,6,4,5)
//降序排序
list11.sortWith((x, y) => {x > y})

val aa = List("张三"->"男", "李四"->"女", "王五"->"男")

aa.groupBy((x: (String, String)) => {x._2})
aa.groupBy(x => x._2)
val stringToTuples: Predef.Map[String, List[(String, String)]] = aa.groupBy(_._2)

stringToTuples.map(x => (x._1, x._2.size))//每个性别的人数

val a6 = List(1,2,3,4,5,6,7,8,9,10)
a6.reduce((x,y) => x + y)
a6.reduce(_ + _) //

def getAddress(a:String)(b:String,c:String):String={
  a+"-"+b+"-"+c
}

getAddress("china")("beijing","tiananmen")

val func = getAddress("china") _
func("beijing","tiananmen")





































