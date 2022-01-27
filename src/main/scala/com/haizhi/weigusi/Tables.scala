package com.haizhi.weigusi

import com.github.aselab.activerecord._
import dsl._
import com.haizhi.weigusi.model.User

object Tables extends ActiveRecordTables{
  val user = table[User]("user")

}
