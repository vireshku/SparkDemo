package com.infy.demo.entry

case class Employee1(firstName:String,lastName:String, email:String,salary:Int)
case class Department(id:Int,name:String)
case class DepartmentWithEmployees(department: Department, employees: Seq[Employee1])