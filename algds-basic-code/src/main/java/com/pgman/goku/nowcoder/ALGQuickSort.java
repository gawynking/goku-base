package com.pgman.goku.nowcoder;

import java.util.Arrays;

/**
 * 快速排序算法
 */
public class ALGQuickSort {

    private int[] quickSort(int[] arr, int left, int right) {
        if (left < right) {
            int partitionIndex = partition(arr, left, right);
            quickSort(arr, left, partitionIndex - 1);
            quickSort(arr, partitionIndex + 1, right);
        }
        return arr;
    }

    private int partition(int[] arr, int left, int right) {
        // 设定基准值（pivot）
        int pivot = left;
        int index = pivot + 1;
        for (int i = index; i <= right; i++) {
            if (arr[i] < arr[pivot]) {
                swap(arr, i, index);
                index++;
            }
        }
        swap(arr, pivot, index - 1);
        return index - 1;
    }

    private void swap(int[] arr, int i, int j) {
        int temp = arr[i];
        arr[i] = arr[j];
        arr[j] = temp;
    }



    public static void main(String[] args) throws Exception {
        int[] sourceArray = {19,8,9,3,5,67,1,20,35,99};

        int[] sort1 = new ALGQuickSort().quickSort(sourceArray,0,sourceArray.length-1);

        for(int i=0;i<sort1.length;i++){
            System.out.print(sort1[i] + " ,");
        }
    }


}
