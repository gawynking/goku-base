package com.pgman.goku.nowcoder;

public class ALGBubbleSort {

    /**
     * 冒泡排序
     *
     * @param sourceArray
     * @return
     */
    public int[] sort(int[] sourceArray){


        for (int i = 1; i < sourceArray.length; i++) {

            for (int j = 0; j < sourceArray.length - i; j++) {
                if (sourceArray[j] > sourceArray[j + 1]) {
                    int tmp = sourceArray[j];
                    sourceArray[j] = sourceArray[j + 1];
                    sourceArray[j + 1] = tmp;
                }
            }

        }

        return sourceArray;
    }

    public static int[] bubbleSort(int[] arr){

        for(int i=1;i<arr.length;i++){// i初始值要向前进一步
            for(int j=0;j<arr.length-i;j++){
                if(arr[j]>arr[j+1]){ // 比较相邻两个元素
                    int tmp = arr[j];
                    arr[j] = arr[j+1];
                    arr[j+1]=tmp;
                }
            }
        }
        return arr;
    }

    public static void main(String[] args) {
        int[] sourceArray = {19,8,9,3,5,67,1,20,35};

        int[] sort1 = new ALGBubbleSort().bubbleSort(sourceArray);

        for(int i=0;i<sort1.length;i++){
            System.out.print(sort1[i] + " ,");
        }

    }

}
