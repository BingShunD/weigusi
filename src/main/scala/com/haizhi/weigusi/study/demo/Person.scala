package com.haizhi.weigusi.study.demo

class Person{
  private var age = 0
  def increment(){age += 1}
  //可以访问另一个对象的私有字段
  def younger(other : Person)= { age < other.age }
}

/*

Scala编译器private和private[this]对做的工作：

（1）对于类私有的字段，Scala生成私有的getter/setter方法；

（2）对于对象私有的字段，Scala不生成getter/setter方法。

*/


//class Dog{
//  private[this]var age = 0
//  def increment(){age += 1}
//  //类似于其他对象.age这样的访问将不被允许
//  //Symbol age is inaccessible from this place
//  def younger(other : Dog)= { age < other.age }
//}


/** A singleton object for the master program. The slaves should not access this. */
private[weigusi] object Cat{

}
