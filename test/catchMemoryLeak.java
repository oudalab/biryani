//package com.crunchify.tutorials;

public class catchMemoryLeak{

   public static void main(String[] args){
   	int mb=1024*1024;
    //get Runtime instanc*
    Runtime instance=Runtime.getRuntime();

    System.out.println("Total Memory:"+instance.totalMemory() / mb);
    System.out.println("Free Memory:"+instance.freeMemory() / mb);
    System.out.println("Used Memory:"+(instance.totalMemory()-instance.freeMemory())/mb);
    System.out.println("Max Memory: "+instance.maxMemory()/mb);

      // System.out.println("hello world");
   }
}