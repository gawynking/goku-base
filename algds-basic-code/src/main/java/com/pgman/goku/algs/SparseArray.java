package com.pgman.goku.algs;

/**
 * 稀疏数组
 * <p>
 * 基本介绍
 * <p>
 * 当一个数组中大部分元素为０，或者为同一个值的数组时，可以使用稀疏数组来保存该数组。
 * <p>
 * 稀疏数组的处理方法是:
 * 记录数组一共有几行几列，有多少个不同的值
 * 把具有不同值的元素的行列及值记录在一个小规模的数组中，从而缩小程序的规模
 */
public class SparseArray {

    public static void main(String[] args) {

        int chessArr[][] = new int[11][11];
        chessArr[1][2] = 1;
        chessArr[2][3] = 2;
        chessArr[4][5] = 2;

        System.out.println("------------------------------");
        System.out.println("原始的二维数组");
        for (int[] row : chessArr) {
            for (int data : row) {
                System.out.printf("%d\t", data);
            }
            System.out.println();
        }

        System.out.println("------------------------------");
        System.out.println("稀疏数组");

        int[][] sparseArray = toTwoDimSparseArray(chessArr);
        for (int[] row : sparseArray) {
            System.out.printf("%d\t%d\t%d\t\n", row[0], row[1], row[2]);
        }

        System.out.println("------------------------------");
        System.out.println("稀疏数组还原后的二维数组");

        int[][] resultArray = parserTwoDimSparseArray(sparseArray);
        for (int[] row : resultArray) {

            for (int data : row) {
                System.out.printf("%d\t", data);
            }
            System.out.println();

        }

    }


    /**
     * 将二维数组转化为稀疏数组
     *
     * @param datas
     */
    public static int[][] toTwoDimSparseArray(int[][] datas) {

        int levelOne = datas.length;
        int levelTwo = 0;
        int sum = 0;
        for (int i = 0; i < levelOne; i++) {

            for (int j = 0; j < datas[i].length; j++) {

                if (datas[i].length > levelTwo) {
                    levelTwo = datas[i].length;
                }

                if (datas[i][j] != 0) {
                    sum++;
                }

            }

        }

        int[][] sparseArray = new int[sum + 1][3];
        sparseArray[0][0] = levelOne;
        sparseArray[0][1] = levelTwo;
        sparseArray[0][2] = sum;

        int count = 1;
        for (int i = 0; i < levelOne; i++) {
            for (int j = 0; j < datas[i].length; j++) {
                if (datas[i][j] != 0) {
                    sparseArray[count][0] = i;
                    sparseArray[count][1] = j;
                    sparseArray[count][2] = datas[i][j];
                    count++;
                }
            }
        }

        return sparseArray;

    }


    /**
     * 从稀疏数组还原原始二维数组
     *
     * @param datas
     * @return
     */
    public static int[][] parserTwoDimSparseArray(int[][] datas) {

        int[][] resultArray = new int[datas[0][0]][datas[0][1]];

        for (int i = 1; i < datas.length; i++) {
            resultArray[datas[i][0]][datas[i][1]] = datas[i][2];
        }

        return resultArray;

    }


}
