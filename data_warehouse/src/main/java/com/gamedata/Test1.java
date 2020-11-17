package com.gamedata;


/**
 * @author Shi Lei
 * @create 2020-11-05
 */
public class Test1 {

  public static void main(String[] args) {

    double high = 100.0;
    int loopTimes = 10;

    double sum = 0.0;
    //下落高度
    double fallingLength = high;
    //反弹高度
    double reboundLength = 0.0;
    for (int i = 0; i < loopTimes; i++) {
      reboundLength = fallingLength / 2.0;
      sum = sum + fallingLength + reboundLength;
      fallingLength = reboundLength;
    }
    System.out.println("总高度=" + sum);
    System.out.println("第十次高度=" + reboundLength);
  }
}
