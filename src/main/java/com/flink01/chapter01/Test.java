package com.flink01.chapter01;

import java.util.HashMap;
import java.util.Scanner;

public class Test {

    public static void main(String[] args) {

        Scanner sc = new Scanner(System.in);
        String str = sc.nextLine();
        HashMap map = new HashMap<Character,Integer>();
        map.put('0',0);
        map.put('1',1);
        map.put('2',2);
        map.put('3',3);
        map.put('4',4);
        map.put('5',5);
        map.put('6',6);
        map.put('7',7);
        map.put('8',8);
        map.put('9',9);
        map.put('A',10);
        map.put('B',11);
        map.put('C',12);
        map.put('D',13);
        map.put('E',14);
        map.put('F',15);

        char[] ch= str.toCharArray();
        int result = 0;
        for(int i =ch.length-1;i>0;i--){
            if(ch[i] != '0' && ch[i] != 'x'){
                if(i == ch.length-1){
                    result = result + (Integer)map.get(ch[i]);
                }else{
                    int temp =(Integer)map.get(ch[i]);
                    for(int j = ch.length-i-1;j>0;j--) {
                        temp = temp * 16;
                    }
                    result = result + temp;
                }
            }
        }
        System.out.println(result);
    }

    public static void sort(int[] arr,int flag){
        for(int i=0;i<arr.length-1;i++){
            for(int j = 0;j<arr.length-i-1;j++){
                if(flag == 0 && arr[j]>arr[j+1]) {
                    swap(arr,j,j+1);
                }else if(flag == 1 && arr[j]< arr[j+1]){
                    swap(arr,j,j+1);
                }
            }
        }
    }

    public static void swap(int[] arr,int i ,int j){
        int temp = arr[j];
        arr[j] = arr[i];
        arr[i] = temp;
    }

}
