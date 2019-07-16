package com.my.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._


object Employees {
  
  def parseEmpLine(line:String) = {
    val fields = line.split(",")
    val eid = fields(0).toInt
    val name = fields(1)+" "+fields(2)
    val mid = fields(3).toInt
    (mid,(name,eid))
  }
  def parseMgrLine(line:String) = {
    val fields = line.split(",")
    val id = fields(0).toInt
    val name = fields(1)+" "+fields(2)
    (id,name)
  }
  def main(args: Array[String]) {
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sc = new SparkContext("local[*]","Employees")
  
  val emp = sc.textFile("/path/to/data-sets/employees")
  val mgr = sc.textFile("/path/to/data-sets/managers")

  val mappedEmp = emp.map(parseEmpLine)
  val mappedMgr = mgr.map(parseMgrLine)
  
  val emp_mgr = mappedEmp.join(mappedMgr).map(x=>(x._2._1._1,x._2._2))
  
  println("========Printing mapped employees/managers=========")
  
  emp_mgr.foreach(println)
  
  }
}