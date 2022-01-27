package com.haizhi.weigusi.model

import com.github.aselab.activerecord.{ActiveRecord, ActiveRecordCompanion}
import org.squeryl.annotations.Column

case class User(
                 @Column("user_id") val userId:Int,
                 var name:String,
                 var password:String,
                 var role:String
               ) extends ActiveRecord

object User extends ActiveRecordCompanion[User]{}
