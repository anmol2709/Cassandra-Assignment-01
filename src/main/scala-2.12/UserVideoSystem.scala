package com.knoldus

import com.datastax.driver.core.Cluster
import org.apache.log4j.Logger


class UserVideoSystem {

  val log = Logger.getLogger(this.getClass)
  val cluster = Cluster.builder().addContactPoint("127.0.0.1").build()
  val session = cluster.connect("user_video_system")

  def createTables: String = {

    val createUserTableQuery = "CREATE TABLE user(email text, password text, userid int ,PRIMARY KEY((email),userid));"
    val createVideoTableQuery = "CREATE TABLE video(video_id int ,video_name text, userid int, year int, PRIMARY KEY((video_name),year));"
    val createYearTableQuery = "CREATE TABLE videobyuserid(video_id int ,video_name text, userid int, year int, PRIMARY KEY((userid),year))"
    session.execute(createUserTableQuery)
    session.execute(createVideoTableQuery)
    session.execute(createYearTableQuery)
    "Tables Created Successfully"

  }

  def insertDataIntoUserTable(email: String, password: String, userid: Int): String = {

    val insertQuery = s"INSERT INTO user(email,password,userid) VALUES('$email','$password',$userid);"
    session.execute(insertQuery)
    "1 row added"

  }

  def insertDataIntoVideoAndYearTable(videoId: Int, videoName: String, userid: Int, year: Int): String = {

    val insertQuery = s"INSERT INTO video(video_id,video_name,userid,year) VALUES($videoId,'$videoName',$userid,$year);"
    val insertQuery2 = s"INSERT INTO videobyuserid(video_id,video_name,userid,year) VALUES($videoId,'$videoName',$userid,$year);"
    session.execute(insertQuery)
    session.execute(insertQuery2)

    "1 row added"

  }


  def userByEmail(email: String) = {

    val res = session.execute(s"SELECT * FROM user where email='$email' ")
    val resultList = res.all.toArray.toList
    resultList.map(row => log.info(row))
  }

  def videoByName(name: String) = {

    val res = session.execute(s"SELECT * FROM video where video_name='$name'")
    val resultList = res.all.toArray.toList
    resultList.map(row => log.info(row))

  }

  def videoByYearAndAbove(year: Int) = {

    val res = session.execute(s"SELECT * FROM video where year>$year ALLOW FILTERING")
    val resultList = res.all.toArray.toList
    resultList.map(row => log.info(row))


  }

  def videoByUseridYearDesc(userid: Int) = {
    val res = session.execute(s"SELECT * FROM videobyuserid where userid=$userid ORDER BY year DESC ")


    val resultList = res.all.toArray.toList

    resultList.map(row => log.info(row))

    cluster.close()
  }


}


