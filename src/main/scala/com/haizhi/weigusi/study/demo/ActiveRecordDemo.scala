package com.haizhi.weigusi.study.demo

import com.github.aselab.activerecord
import com.github.aselab.activerecord.ActiveRecord
import com.haizhi.weigusi.Tables
import com.haizhi.weigusi.model.User
import com.github.aselab.activerecord.dsl._

import scala.collection.immutable

object ActiveRecordDemo {
  private val configFile = "conf/application.conf"
  System.setProperty("config.file", configFile)
  def main(args: Array[String]): Unit = {
    Tables.initialize
    val maybeUser: Option[User] = User.findBy("name", "wangwu")
    val value = maybeUser.get
    println(value)
    println("=======")

    val allByName: ActiveRecord.Relation1[User, User] = User.findAllBy("name", "wangwu")
    val allByNameList: immutable.Seq[User] = allByName.toList
    allByNameList.foreach(println)
    println("=======")

    val value1: activerecord.ActiveRecord.Relation1[User, User] = User.where(_.userId.~ > 5).orderBy(_.userId desc)
    val res = value1.toList
    res.foreach(println)



//    User(4,"wangwu","ww123","worker").save()
//    User(5,"zhaoliu","zl123","worker").save()
//    User(6,"maqi","mq123","worker").save()

    // Person.findBy("name", "person1") //=> Some(Person("person1", 25))
    // Person.findBy("age", 55) //=> None
    // Person.findAllBy("age", 18).toList //=> List(Person("person2", 18), Person("person4", 18))
    // Person.where(_.age.~ >= 20).orderBy(_.age desc).toList //=> List(Person("person3", 40), Person("person1", 25))


    Tables.cleanup
  }
}
