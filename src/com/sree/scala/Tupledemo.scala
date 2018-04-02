package com.sree.scala

object FunctionPassing {
  
  
    def squareInt(x:Int) : Int ={
       x*x
      
    }
    
    def testmethodpassing (x: Int, f: Int=>Int):Int={
      f(x)
    }
    
    def main(args: Array[String]): Unit ={
      
       val result = testmethodpassing(2, squareInt)
       
       result match{
        
         case 4 => println("got the output matched from funtion")
          case _ => println("default match")
       }

      println(result)
      
    }
   
    
}