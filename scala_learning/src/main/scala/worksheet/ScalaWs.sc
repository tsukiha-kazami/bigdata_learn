//使用val声明变量,相当于java中的final修饰,不能在指向其他的数据了
val a: Int = 10

//使用var声明变量,后期可以被修改重新赋值
var b: Int = 20
b = 100

//scala中的变量的类型可以显式的声明,也可以不声明,如果不显式的声明这会根据变量的值来推断出来变量的类型(scala支持类型推断)
val c = 20
///* Scala中使用==关键字lazy==来定义惰性变量，实现延迟加载(懒加载)。
//* 惰性变量只能是==不可变变量==，并且只有在调用惰性变量时，才会去实例化这个变量
lazy val a = 1


//定义
val x = 1

//if表达式
val y = if (x > 0) 1 else -1

//支持混合类型表达式
val z = if (x > 1) 1 else "error"

//缺失else 相当于 if(x>2) 1 else ()
val m = if (x > 2) 1

//scala中有个Unit类，用作不返回任何结果的方法的结果类型,相当于Java中的void，Unit只有一个实例值，写成()
val n = if (x > 2) 1 else ()


//if(xx) else if(xx) val
val k = if (x < 0) -1 else if (x == 0) 0 else 1


//result的值就是块表达式的结果
//后期一个方法的返回值不需要加上return,把要返回的结果放在方法的最后一行就可以了
val x = 0
val result = {
  val y = x + 10
  val z = y + "-hello"
  val m = z + "-kaikeba"
  "over"
}


val nums = 1 to 10

for (i <- nums) println(i)


for (i <- 1 to 3; j <- 1 to 2) {
  println(i * 10 + j + "\t")
}


for (i <- 1 to 9; j <- i to 9) {
  if (i == j) {
    println()
  }
  print(i + "*" + j + "=" + i * j + "\t\t")
}

for ( i<- 1 to 10 if i%2 ==0){
  println(i)
}

// for推导式：for表达式中以yield开始，该for表达式会构建出一个集合

val v = for(i <- 1 to 5) yield i * 10

var x = 10
while(x >5){
  println(x)
  x -= 1
}


def hello (first:String ,second :Int):Int ={
  second
}

def  hello2(first:Int , second:String) ={
  //println(first)
  //20
}
val hello2Result = hello2(20,"abc")
println( hello2Result)

//示例三：定义一个方法，不定义返回值，可以通过自动推断，返回不同类型的值
def hello3(first:Int,second:String) ={
  if(first > 10){
    first
  }else{
    second
  }
}
val hello3Result = hello3(5,"helloworld")
println(hello3Result)



//定义一个方法，参数给定默认值，如果不传入参数，就使用默认值来代替

def hello4(first:Int = 10,second:String)={
  println(first+"\t"+ second)
}
//注意我们在调用方法的时候我们可以通过参数名来指定我们的参数的值
hello4(second="helloworld")


//变长参数，方法的参数个数不定的，类似于java当中的方法的...可变参数
def hello5(first:Int*)={
  var result = 0;
  for(arg <- first){
    result  += arg
  }
  println(result)
}
hello5(10,20,30)
hello5(10,50)



//递归函数。我们可以定义一个方法，使得方法自己调用自己，形成一个递归函数，但是方法的==返回值类型必须显示的手动指定==
def  hello6(first:Int):Int={
  if(first <= 1){
    1
  }else{
    first * hello6(first -1)
  }
}

val hello6Result = hello6(10)
println(hello6Result)



/**
 * 定义了一个方法，但是方法的返回值没有显示指定，
 * 此时我们就可以省掉方法定义的=号，如果省掉 = 号，
 * 那么这个方法强调的是一个过程，代码执行的过程，
 * 不会产生任何的返回值
 * @param first
 */
def hello7(first:Int){
  println(first)
  30
}
hello7(20)



val func1 =(x:Int,y:Int) =>{
  x+y
}
func1(2,8)

(x:Int,y:String) =>{x + y}

val func3 :Int => Int = {x => x * x }
val func3Result = func3(10)

val func4:(Int,String) =>(String,Int) ={
  (x,y) => (y,x)
}
val func4Result = func4(10,"hello")
println(func4Result)

//方法转换为函数
def add(x:Int,y:Int)=x+y

val a = add _
a(2,3)



val a=new Array[Int](10)
a(0)
a(0)=10
a

val b =Array("hadoop","spark","hive")
b(0)
b.length


import scala.collection.mutable.ArrayBuffer

val a = ArrayBuffer[Int]()
val b1= ArrayBuffer("1","2","3")

b1+="4"+="5"
println(b1)


b1 -= "1"
println(b1)

b1 ++= Array("6","7")
println(b1)


b1.insert(1,"aa")
println(b1)
b1.insert(1,"bb","cc")
println(b1)
b1.remove(2)
println(b1)
//从2开始 删2个
b1.remove(2,2)
println(b1)

//去除最后2个元素
b1.trimEnd(2)


for(i <- b1)println(i)


for(i <- 0 to b1.length -1 )println(b1(i))
//to 包含最大值，util不包含
for(i <- 0 until b1.length) println(b1(i))

for(i <- 0 until (b1.length,2)) println(b1(i))


var a = (1, "张三", 20, "北京市")
val b = 1->2->4
println(b)

a._1


val map1 = Map("zhangsan"->30, "lisi"->40)

val map2 = Map(("zhangsan", 30), ("lisi", 30))

map1("zhangsan")
//不可变的map中的value不可变
import scala.collection.mutable.Map

val map3 = Map("zhangsan"->30, "lisi"->40)
map3("zhangsan")
map3("zhangsan")=50
map3


map3.getOrElse("wangwu", -1)
map3 += ("wangwu" ->35)
for(k <- map3.keys) println(k+" -> " +map3(k))

for((k,v) <- map3) println(k+" -> "+v)

//注意：这里对不可变的set集合进行添加删除等操作，对于该集合来说是没有发生任何变化，这里是生成了新的集合，新的集合相比于原来的集合来说发生了变化。
val a = Set(1,1,2,3,4,5)

a.size
for(i <- a) println(i)
a + 6
a - 1
//删除set集合中存在的元素
a -- Set(2,3)
// 拼接两个集合
a ++ Set(6,7,8)
//求2个Set集合的交集
a & Set(3,4,5,6)



import scala.collection.mutable.Set

val set=Set(1,2,3,4,5)
set +=6
set +=(6,7,8,9)
set ++=Set(10,11)



val  list1=List(1,2,3,4)
//使用Nil创建一个不可变的空列表
val  list2=Nil
//使用 :: 方法创建列表，包含1、2、3三个元素
val list3=1::2::3::Nil


import scala.collection.mutable.ListBuffer

val list=ListBuffer(1,2,3,4)
list(0)
list.head
list.tail
list +=5
list ++=List(6,7)
list ++=ListBuffer(8,9)
list -=9
list --=List(7,8)
list --=ListBuffer(5,6)
list.toList
list.toArray
