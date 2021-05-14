package com.infy.demo.entry

case class Employee(EmpId: String, Experience: Double, Salary: Double)

case class Employee2(EmpId: EmpData, Experience: EmpData, Salary: EmpData)
case class EmpData(key: String,value:String)