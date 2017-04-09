package com.knoldus

import org.apache.log4j.Logger


object QuerySystem extends App {

  val log = Logger.getLogger(this.getClass)

  val systemObject = new UserVideoSystem
  val ref = systemObject.createTables
log.info("\n: : CREATING TABLES : : ")
log.info(ref)
  val user1 = systemObject.insertDataIntoUserTable("anmol.sarna@knoldus.in", "anmol", 1)
  val user2 = systemObject.insertDataIntoUserTable("anmol.sarna27@gmail.com", "anmol123", 2)
  val user3 = systemObject.insertDataIntoUserTable("anmol@gmail.com", "anmol1234", 3)

  log.info("\n: : INSERTING DATA TO USER TABLE : : ")
  log.info("User : " + user1)
  log.info("User : " + user2)
  log.info("User : " + user3)
  val video1 = systemObject.insertDataIntoVideoAndYearTable(1, "Humma Humma", 1, 2016)
  val video2 = systemObject.insertDataIntoVideoAndYearTable(2, "Ik Vaari", 1, 2015)
  val video3 = systemObject.insertDataIntoVideoAndYearTable(3, "Tum hi ho", 1, 2014)
  val video4 = systemObject.insertDataIntoVideoAndYearTable(4, "Fifa 15 GamePlay", 2, 2012)
  val video5 = systemObject.insertDataIntoVideoAndYearTable(5, "Halo 3 GamePlay", 2, 2014)
  val video6 = systemObject.insertDataIntoVideoAndYearTable(6, "Assassins Creed GamePlay", 2, 2015)
  val video7 = systemObject.insertDataIntoVideoAndYearTable(7, "Scala Tutorials", 3, 2016)
  val video8 = systemObject.insertDataIntoVideoAndYearTable(8, "Akka Tutorials", 3, 2017)
  val video9 = systemObject.insertDataIntoVideoAndYearTable(9, "Cassandra Tutorials", 3, 2014)

  log.info("\n: : INSERTING DATA TO VIDEO AND YEAR TABLE : : ")

  log.info(video1)
  log.info(video2)
  log.info(video3)
  log.info(video4)
  log.info(video5)
  log.info(video6)
  log.info(video7)
  log.info(video8)
  log.info(video9)


  log.info("\nFinding User By Email :")
  systemObject.userByEmail("anmol.sarna@knoldus.in")
  log.info("\nFinding Video By Name :")
  systemObject.videoByName("Scala Tutorials")
  log.info("\nFinding Video By Year And Above:")
  systemObject.videoByYearAndAbove(2015)
  log.info("\nFinding Video By UserID And Year in Descending order :")
  systemObject.videoByUseridYearDesc(2)

}