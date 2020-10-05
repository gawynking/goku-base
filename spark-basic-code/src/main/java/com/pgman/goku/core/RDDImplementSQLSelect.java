//package com.pgman.goku.core;
//
//import java.util.Arrays;
//import java.util.HashMap;
//import java.util.Iterator;
//import java.util.Map;
//import org.apache.spark.SparkConf;
//import org.apache.spark.api.java.JavaPairRDD;
//import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.api.java.JavaSparkContext;
//import org.apache.spark.api.java.Optional;
//import org.apache.spark.api.java.function.Function;
//import org.apache.spark.api.java.function.PairFunction;
//import org.apache.spark.api.java.function.VoidFunction;
//
//import com.pgman.goku.utils.*;
//import scala.Tuple2;
//
///**
// * 应用Spark Core RDD实现常规SQL select算法
// *
// * @author ChavinKing
// */
//
//public class RDDImplementSQLSelect {
//
//    public static void main(String[] args) {
//        // TODO Auto-generated method stub
//        System.setProperty("hadoop.home.dir", "D:\\hadoop-common-2.2.0-bin");
//
//        selectFrom();
////        selectFromWhere();
////        selectFromOrderOne();
////        selectFromGroupByKey();
////        selectFromGroupByKeyHaving();
////        selectFromGroupTopN(1);
////        selectUnionAll();
////        selectUnion();
////        selectJoinWhere();
////        selectJoin();
////        selectLeftJoin();
////        selectFromOrderTwo();
//
//    }
//
//    /**
//     * RDD模拟以下SQL实现： select * from emp e;
//     */
//    public static void selectFrom() {
//
//        SparkConf conf = new SparkConf().setAppName("selectFrom"); //.setMaster("local");
//        JavaSparkContext sc = new JavaSparkContext(conf);
//
//        JavaRDD<String> linesRDD = sc.textFile("hdfs://chavin.king:8020/files/emp.csv");
//
//        linesRDD.foreach(new VoidFunction<String>() {
//
//            public void call(String line) throws Exception {
//                // TODO Auto-generated method stub
//
//                System.out.println(line.replaceAll(",", "\t"));
//
//            }
//        });
//
//        sc.close();
//    }
//
//    /**
//     * RDD模拟以下SQL实现： select * from emp e where e.sal >= 3000;
//     */
//    public static void selectFromWhere() {
//
//        SparkConf conf = new SparkConf().setAppName("selectFromWhere");
//        JavaSparkContext sc = new JavaSparkContext(conf);
//
//        JavaRDD<String> linesRDD = sc.textFile("hdfs://chavin.king:8020/files/emp.csv");
//
//        JavaRDD<String> filterRDD = linesRDD.filter(new Function<String, Boolean>() {
//
//            public Boolean call(String line) throws Exception {
//                // TODO Auto-generated method stub
//                return Integer.valueOf(line.split(",")[5]) >= 3000;
//            }
//        });
//
//        filterRDD.foreach(new VoidFunction<String>() {
//
//            public void call(String line) throws Exception {
//                // TODO Auto-generated method stub
//
//                System.out.println(line.replaceAll(",", "\t"));
//
//            }
//        });
//
//        sc.close();
//
//    }
//
//    /**
//     * RDD模拟以下SQL实现： select * from emp e order by e.sal desc;
//     */
//    public static void selectFromOrderOne() {
//
//        SparkConf conf = new SparkConf().setAppName("selectFromOrderOne").setMaster("local");
//        JavaSparkContext sc = new JavaSparkContext(conf);
//
//        JavaRDD<String> linesRDD = sc.textFile("files//emp.csv");
//
//        JavaPairRDD<Integer, String> pairsRDD = linesRDD.mapToPair(new PairFunction<String, Integer, String>() {
//            public Tuple2<Integer, String> call(String line) throws Exception {
//                // TODO Auto-generated method stub
//                int sal = Integer.valueOf(line.split(",")[5]);
//                return new Tuple2<Integer, String>(sal, line);
//            }
//        });
//
//        JavaPairRDD<Integer, String> pairsSortRDD = pairsRDD.sortByKey(false);
//
//        pairsSortRDD.foreach(new VoidFunction<Tuple2<Integer, String>>() {
//
//            public void call(Tuple2<Integer, String> t) throws Exception {
//                // TODO Auto-generated method stub
//                System.out.println(t._2.replaceAll(",", "\t"));
//            }
//        });
//
//        sc.close();
//
//    }
//
//    /**
//     * RDD模拟以下SQL实现： select * from emp e order by e.deptno desc,e.sal desc;
//     */
//    public static void selectFromOrderTwo() {
//
//        SparkConf conf = new SparkConf().setAppName("selectFromOrderTwo").setMaster("local");
//        JavaSparkContext sc = new JavaSparkContext(conf);
//
//        JavaRDD<String> linesRDD = sc.textFile("files//emp.csv");
//
//        /**
//         * SecondarySortKey 为自定义的排序规则类
//         */
//        JavaPairRDD<SecondarySortKey, String> pairSortRDD = linesRDD.mapToPair(new PairFunction<String, SecondarySortKey, String>() {
//            public Tuple2<SecondarySortKey, String> call(String line) throws Exception {
//                // TODO Auto-generated method stub
//
//                SecondarySortKey sortKey = new SecondarySortKey(Integer.valueOf(line.split(",")[7]), Integer.valueOf(line.split(",")[5]));
//                return new Tuple2<SecondarySortKey, String>(sortKey, line);
//            }
//        });
//
//        JavaPairRDD<SecondarySortKey, String> pairSortedRDD = pairSortRDD.sortByKey(false);
//
//        pairSortedRDD.foreach(new VoidFunction<Tuple2<SecondarySortKey, String>>() {
//
//            public void call(Tuple2<SecondarySortKey, String> t) throws Exception {
//                // TODO Auto-generated method stub
//                System.out.println(t._2.replaceAll(",", "\t"));
//            }
//        });
//
//
//        sc.close();
//
//    }
//
//    /**
//     * RDD模拟以下SQL实现：
//     * <p>
//     * select e.deptno,
//     * sum(e.sal) as sal_total,
//     * max(e.sal) as sal_max,
//     * min(e.sal) as sal_min,
//     * avg(e.sal) as sal_avg,
//     * count(1) as emp_count,
//     * max(e.comm) as comm_max,
//     * min(e.comm) as comm_min
//     * from emp e
//     * group by e.deptno
//     * ;
//     */
//    public static void selectFromGroupByKey() {
//
//        SparkConf conf = new SparkConf().setAppName("selectFromGroupByKey").setMaster("local");
//        JavaSparkContext sc = new JavaSparkContext(conf);
//
//        JavaRDD<String> linesRDD = sc.textFile("files//emp.csv");
//
//        JavaPairRDD<Integer, String> pairsRDD = linesRDD.mapToPair(new PairFunction<String, Integer, String>() {
//            public Tuple2<Integer, String> call(String line) throws Exception {
//                int deptno = Integer.valueOf(line.split(",")[7]);
//                String salANDcomm = line.split(",")[5] + "_" + line.split(",")[6];
//                return new Tuple2<Integer, String>(deptno, salANDcomm);
//            }
//
//            ;
//        });
//
//        JavaPairRDD<Integer, Iterable<String>> pairsGroupByKeyRDD = pairsRDD.groupByKey();
//
//        JavaPairRDD<Integer, Map<String, String>> pairsGroupByKeyMapRDD =
//                pairsGroupByKeyRDD.mapToPair(new PairFunction<Tuple2<Integer, Iterable<String>>, Integer, Map<String, String>>() {
//                    public Tuple2<Integer, Map<String, String>> call(Tuple2<Integer, Iterable<String>> t) throws Exception {
//                        // TODO Auto-generated method stub
//
//                        int sal_total = 0;
//                        int sal_max = 0;
//                        int sal_min = 0;
//                        float sal_avg = 0;
//                        int emp_count = 0;
//                        int comm_max = 0;
//                        int comm_min = 0;
//
//                        int deptno = t._1;
//
//                        Map<String, String> resultMap = new HashMap<String, String>();
//
//                        Iterator<String> iter = t._2.iterator();
//
//                        while (iter.hasNext()) {
//
//                            String[] tmp = iter.next().split("_");
//                            int sal = Integer.valueOf(tmp[0]);
//
//                            int comm;
//                            try {
//                                comm = Integer.valueOf(tmp[1]);
//                            } catch (ArrayIndexOutOfBoundsException e) {
//                                comm = 0;
//                            }
//
//                            sal_total += sal;
//
//                            if (sal >= sal_max) {
//                                sal_max = sal;
//                            }
//
//                            if (sal <= sal_min || sal_min == 0) {
//                                sal_min = sal;
//                            }
//
//                            emp_count += 1;
//
//                            if (comm >= comm_max) {
//                                comm_max = comm;
//                            }
//
//                            if (comm <= comm_min) {
//                                comm_min = comm;
//                            }
//
//                        }
//
//                        sal_avg = sal_total / emp_count;
//
//                        resultMap.put("sal_total", String.valueOf(sal_total));
//                        resultMap.put("sal_max", String.valueOf(sal_max));
//                        resultMap.put("sal_min", String.valueOf(sal_min));
//                        resultMap.put("sal_avg", String.valueOf(sal_avg));
//                        resultMap.put("emp_count", String.valueOf(emp_count));
//                        resultMap.put("comm_max", String.valueOf(comm_max));
//                        resultMap.put("comm_min", String.valueOf(comm_min));
//
//                        return new Tuple2<Integer, Map<String, String>>(deptno, resultMap);
//                    }
//                });
//
//        pairsGroupByKeyMapRDD.foreach(new VoidFunction<Tuple2<Integer, Map<String, String>>>() {
//
//            public void call(Tuple2<Integer, Map<String, String>> t) throws Exception {
//                // TODO Auto-generated method stub
//                System.out.println(
//                        t._1 + "\t" +
//                                t._2.get("sal_total") + "\t" +
//                                t._2.get("sal_max") + "\t" +
//                                t._2.get("sal_min") + "\t" +
//                                t._2.get("sal_avg") + "\t" +
//                                t._2.get("emp_count") + "\t" +
//                                t._2.get("comm_max") + "\t" +
//                                t._2.get("comm_min")
//                );
//            }
//        });
//
//        sc.close();
//
//    }
//
//    /**
//     * RDD模拟以下SQL实现：
//     * <p>
//     * select e.deptno,
//     * sum(e.sal) as sal_total,
//     * max(e.sal) as sal_max,
//     * min(e.sal) as sal_min,
//     * avg(e.sal) as sal_avg,
//     * count(1) as emp_count,
//     * max(e.comm) as comm_max,
//     * min(e.comm) as comm_min
//     * from emp e
//     * group by e.deptno
//     * having count(1) > 5
//     * ;
//     */
//    public static void selectFromGroupByKeyHaving() {
//
//        SparkConf conf = new SparkConf().setAppName("selectFromGroupByKeyHaving").setMaster("local");
//        JavaSparkContext sc = new JavaSparkContext(conf);
//
//        JavaRDD<String> linesRDD = sc.textFile("files//emp.csv");
//
//        JavaPairRDD<Integer, String> pairsRDD = linesRDD.mapToPair(new PairFunction<String, Integer, String>() {
//            public Tuple2<Integer, String> call(String line) throws Exception {
//                int deptno = Integer.valueOf(line.split(",")[7]);
//                String salANDcomm = line.split(",")[5] + "_" + line.split(",")[6];
//                return new Tuple2<Integer, String>(deptno, salANDcomm);
//            }
//
//            ;
//        });
//
//        JavaPairRDD<Integer, Iterable<String>> pairsGroupByKeyRDD = pairsRDD.groupByKey();
//
//        JavaPairRDD<Integer, Map<String, String>> pairsGroupByKeyMapRDD =
//                pairsGroupByKeyRDD.mapToPair(new PairFunction<Tuple2<Integer, Iterable<String>>, Integer, Map<String, String>>() {
//                    public Tuple2<Integer, Map<String, String>> call(Tuple2<Integer, Iterable<String>> t) throws Exception {
//                        // TODO Auto-generated method stub
//
//                        int sal_total = 0;
//                        int sal_max = 0;
//                        int sal_min = 0;
//                        float sal_avg = 0;
//                        int emp_count = 0;
//                        int comm_max = 0;
//                        int comm_min = 0;
//
//                        int deptno = t._1;
//
//                        Map<String, String> resultMap = new HashMap<String, String>();
//
//                        Iterator<String> iter = t._2.iterator();
//
//                        while (iter.hasNext()) {
//
//                            String[] tmp = iter.next().split("_");
//                            int sal = Integer.valueOf(tmp[0]);
//
//                            int comm;
//                            try {
//                                comm = Integer.valueOf(tmp[1]);
//                            } catch (ArrayIndexOutOfBoundsException e) {
//                                comm = 0;
//                            }
//
//                            sal_total += sal;
//
//                            if (sal >= sal_max) {
//                                sal_max = sal;
//                            }
//
//                            if (sal <= sal_min || sal_min == 0) {
//                                sal_min = sal;
//                            }
//
//                            emp_count += 1;
//
//                            if (comm >= comm_max) {
//                                comm_max = comm;
//                            }
//
//                            if (comm <= comm_min) {
//                                comm_min = comm;
//                            }
//
//                        }
//
//                        sal_avg = sal_total / emp_count;
//
//                        resultMap.put("sal_total", String.valueOf(sal_total));
//                        resultMap.put("sal_max", String.valueOf(sal_max));
//                        resultMap.put("sal_min", String.valueOf(sal_min));
//                        resultMap.put("sal_avg", String.valueOf(sal_avg));
//                        resultMap.put("emp_count", String.valueOf(emp_count));
//                        resultMap.put("comm_max", String.valueOf(comm_max));
//                        resultMap.put("comm_min", String.valueOf(comm_min));
//
//                        return new Tuple2<Integer, Map<String, String>>(deptno, resultMap);
//                    }
//                });
//
//        JavaPairRDD<Integer, Map<String, String>> pairsGroupByKeyMapFilterRDD = pairsGroupByKeyMapRDD.filter(new Function<Tuple2<Integer, Map<String, String>>, Boolean>() {
//
//            public Boolean call(Tuple2<Integer, Map<String, String>> t) throws Exception {
//                // TODO Auto-generated method stub
//                return Integer.valueOf(t._2.get("emp_count")) > 5;
//            }
//        });
//
//
//        pairsGroupByKeyMapFilterRDD.foreach(new VoidFunction<Tuple2<Integer, Map<String, String>>>() {
//
//            public void call(Tuple2<Integer, Map<String, String>> t) throws Exception {
//                // TODO Auto-generated method stub
//                System.out.println(
//                        t._1 + "\t" +
//                                t._2.get("sal_total") + "\t" +
//                                t._2.get("sal_max") + "\t" +
//                                t._2.get("sal_min") + "\t" +
//                                t._2.get("sal_avg") + "\t" +
//                                t._2.get("emp_count") + "\t" +
//                                t._2.get("comm_max") + "\t" +
//                                t._2.get("comm_min")
//                );
//            }
//        });
//
//        sc.close();
//
//    }
//
//    /**
//     * RDD模拟以下SQL实现：
//     * <p>
//     * select tmp.empno,
//     * tmp.ename,
//     * tmp.job,
//     * tmp.sal,
//     * tmp.deptno
//     * from (
//     * select e.*,
//     * row_number() over(partition by e.deptno order by e.sal desc) as rn
//     * from emp e
//     * ) tmp
//     * where tmp.rn <= 3;
//     */
//    public static void selectFromGroupTopN(final int n) {
//
//        SparkConf conf = new SparkConf().setAppName("selectFromGroupTopN").setMaster("local");
//        JavaSparkContext sc = new JavaSparkContext(conf);
//
//        JavaRDD<String> linesRDD = sc.textFile("files//emp.csv");
//
//        JavaPairRDD<Integer, String> pairsRDD = linesRDD.mapToPair(new PairFunction<String, Integer, String>() {
//            public Tuple2<Integer, String> call(String t) throws Exception {
//                // TODO Auto-generated method stub
//
//                String[] tmp = t.split(",");
//
//                int deptno = Integer.valueOf(tmp[7]);
//                String columns = tmp[0] + "_" + tmp[1] + "_" + tmp[2] + "_" + tmp[5];
//
//                return new Tuple2<Integer, String>(deptno, columns);
//            }
//        });
//
//        JavaPairRDD<Integer, Iterable<String>> pairsGroupRDD = pairsRDD.groupByKey();
//
//        JavaPairRDD<Integer, Iterable<String>> pairsGroupMapRDD = pairsGroupRDD.mapToPair(new PairFunction<Tuple2<Integer, Iterable<String>>, Integer, Iterable<String>>() {
//            public Tuple2<Integer, Iterable<String>> call(Tuple2<Integer, Iterable<String>> t) throws Exception {
//                // TODO Auto-generated method stub
//
//                Integer[] topN = new Integer[n];
//
//                int deptno = t._1;
//
//                String[] result = new String[n];
//
//                Iterator<String> iter = t._2.iterator();
//
//                while (iter.hasNext()) {
//
//                    String[] tmp = iter.next().split("_");
//
//                    int sal = Integer.valueOf(tmp[3]);
//
//                    /**
//                     * top N算法
//                     */
//                    for (int i = 0; i < n; i++) {
//
//                        if (topN[i] == null) {
//                            topN[i] = sal;
//                            result[i] = topN[i] + "_" + tmp[0] + "_" + tmp[1] + "_" + tmp[2];
//                            break;
//                        } else if (sal > topN[i]) {
//                            for (int j = n - 1; j > i; j--) {
//                                topN[j] = topN[j - 1];
//                                result[j] = result[j - 1];
//                            }
//
//                            topN[i] = sal;
//                            result[i] = topN[i] + "_" + tmp[0] + "_" + tmp[1] + "_" + tmp[2];
//                            break;
//                        }
//
//
//                    }
//
//                }
//
//                return new Tuple2<Integer, Iterable<String>>(deptno, Arrays.asList(result));
//            }
//        });
//
//        pairsGroupMapRDD.foreach(new VoidFunction<Tuple2<Integer, Iterable<String>>>() {
//
//            public void call(Tuple2<Integer, Iterable<String>> t) throws Exception {
//                // TODO Auto-generated method stub
//
//                int deptno = t._1;
//                Iterator<String> iter = t._2.iterator();
//
//                while (iter.hasNext()) {
//
//                    String tmp = iter.next();
//
//                    System.out.println(deptno + "\t" + tmp.replaceAll("_", "\t"));
//                }
//
//            }
//        });
//
//        sc.close();
//
//    }
//
//    /**
//     * RDD模拟以下SQL实现：
//     * <p>
//     * select empno,ename,sal,deptno
//     * from emp e
//     * where e.deptno = 10
//     * <p>
//     * union all
//     * <p>
//     * select empno,ename,sal,deptno
//     * from emp e
//     * where e.deptno = 10
//     * ;
//     */
//
//    @SuppressWarnings("serial")
//    public static void selectUnionAll() {
//
//        SparkConf conf = new SparkConf().setAppName("selectUnionAll").setMaster("local");
//        JavaSparkContext sc = new JavaSparkContext(conf);
//
//        JavaRDD<String> linesRDD = sc.textFile("files//emp.csv");
//
//        JavaRDD<String> union1 = linesRDD.filter(new Function<String, Boolean>() {
//
//            public Boolean call(String line) throws Exception {
//                // TODO Auto-generated method stub
//                return Integer.valueOf(line.split(",")[7]) == 10;
//            }
//        });
//
//        JavaRDD<String> union2 = linesRDD.filter(new Function<String, Boolean>() {
//
//            public Boolean call(String line) throws Exception {
//                // TODO Auto-generated method stub
//                return Integer.valueOf(line.split(",")[7]) == 10;
//            }
//        });
//
//        JavaRDD<String> unionAll = union1.union(union2);
//
//        unionAll.foreach(new VoidFunction<String>() {
//
//            public void call(String t) throws Exception {
//                // TODO Auto-generated method stub
//                System.out.println(t.replaceAll(",", "\t"));
//            }
//        });
//
//
//        sc.close();
//
//    }
//
//
//    /**
//     * RDD模拟以下SQL实现：
//     * <p>
//     * select empno,ename,sal,deptno
//     * from emp e
//     * where e.deptno = 10
//     * <p>
//     * union
//     * <p>
//     * select empno,ename,sal,deptno
//     * from emp e
//     * where e.deptno = 10
//     * ;
//     */
//
//    @SuppressWarnings("serial")
//    public static void selectUnion() {
//
//        SparkConf conf = new SparkConf().setAppName("selectUnion").setMaster("local");
//        JavaSparkContext sc = new JavaSparkContext(conf);
//
//        JavaRDD<String> linesRDD = sc.textFile("files//emp.csv");
//
//        JavaRDD<String> union1 = linesRDD.filter(new Function<String, Boolean>() {
//
//            public Boolean call(String line) throws Exception {
//                // TODO Auto-generated method stub
//                return Integer.valueOf(line.split(",")[7]) == 10;
//            }
//        });
//
//        JavaRDD<String> union2 = linesRDD.filter(new Function<String, Boolean>() {
//
//            public Boolean call(String line) throws Exception {
//                // TODO Auto-generated method stub
//                return Integer.valueOf(line.split(",")[7]) == 10;
//            }
//        });
//
//        JavaRDD<String> union = union1.union(union2).distinct();
//
//        union.foreach(new VoidFunction<String>() {
//
//            public void call(String t) throws Exception {
//                // TODO Auto-generated method stub
//                System.out.println(t.replaceAll(",", "\t"));
//            }
//        });
//
//
//        sc.close();
//
//    }
//
//    /**
//     * RDD模拟以下SQL实现：
//     * <p>
//     * select e.empno,e.ename,d.deptno,d.dname,e.sal
//     * from emp e join dept d on e.deptno = d.deptno
//     * ;
//     */
//
//    @SuppressWarnings("serial")
//    public static void selectJoin() {
//
//        SparkConf conf = new SparkConf().setAppName("selectJoin").setMaster("local");
//        JavaSparkContext sc = new JavaSparkContext(conf);
//
//        JavaRDD<String> deptRDD = sc.textFile("files//dept.csv");
//        JavaRDD<String> empRDD = sc.textFile("files//emp.csv");
//
//        JavaPairRDD<Integer, String> deptMapRDD = deptRDD.mapToPair(new PairFunction<String, Integer, String>() {
//            public Tuple2<Integer, String> call(String line) throws Exception {
//                // TODO Auto-generated method stub
//                return new Tuple2<Integer, String>(Integer.valueOf(line.split(",")[0]), line);
//            }
//        });
//
//        JavaPairRDD<Integer, String> empMapRDD = empRDD.mapToPair(new PairFunction<String, Integer, String>() {
//            public Tuple2<Integer, String> call(String line) throws Exception {
//                // TODO Auto-generated method stub
//                return new Tuple2<Integer, String>(Integer.valueOf(line.split(",")[7]), line);
//            }
//        });
//
//        JavaPairRDD<Integer, Tuple2<String, String>> joinRDD = empMapRDD.join(deptMapRDD);
//
//        joinRDD.foreach(new VoidFunction<Tuple2<Integer, Tuple2<String, String>>>() {
//
//            public void call(Tuple2<Integer, Tuple2<String, String>> t) throws Exception {
//                // TODO Auto-generated method stub
//
//                System.out.println(
//
//                        t._2._1.split(",")[0] + "\t" +
//                                t._2._1.split(",")[1] + "\t" +
//                                t._2._2.split(",")[0] + "\t" +
//                                t._2._2.split(",")[1] + "\t" +
//                                t._2._1.split(",")[5]
//
//                );
//
//            }
//        });
//
//        sc.close();
//
//    }
//
//    /**
//     * RDD模拟以下SQL实现：
//     * <p>
//     * select e.empno,e.ename,d.deptno,d.dname,e.sal
//     * from emp e join dept d on e.deptno = d.deptno and d.deptno != 10
//     * where e.sal >= 3000
//     * ;
//     */
//
//    @SuppressWarnings("serial")
//    public static void selectJoinWhere() {
//
//        SparkConf conf = new SparkConf().setAppName("selectJoinWhere").setMaster("local");
//        JavaSparkContext sc = new JavaSparkContext(conf);
//
//        JavaRDD<String> deptRDD = sc.textFile("files//dept.csv");
//        JavaRDD<String> empRDD = sc.textFile("files//emp.csv");
//
//        JavaRDD<String> deptFilterRDD = deptRDD.filter(new Function<String, Boolean>() {
//
//            public Boolean call(String line) throws Exception {
//                // TODO Auto-generated method stub
//                return Integer.valueOf(line.split(",")[0]) != 10;
//            }
//        });
//
//        JavaRDD<String> empFilterRDD = empRDD.filter(new Function<String, Boolean>() {
//
//            public Boolean call(String line) throws Exception {
//                // TODO Auto-generated method stub
//                return Integer.valueOf(line.split(",")[5]) >= 3000;
//            }
//        });
//
//        JavaPairRDD<Integer, String> deptFilterMapRDD = deptFilterRDD.mapToPair(new PairFunction<String, Integer, String>() {
//            public Tuple2<Integer, String> call(String line) throws Exception {
//                // TODO Auto-generated method stub
//                return new Tuple2<Integer, String>(Integer.valueOf(line.split(",")[0]), line);
//            }
//        });
//
//        JavaPairRDD<Integer, String> empFilterMapRDD = empFilterRDD.mapToPair(new PairFunction<String, Integer, String>() {
//            public Tuple2<Integer, String> call(String line) throws Exception {
//                // TODO Auto-generated method stub
//                return new Tuple2<Integer, String>(Integer.valueOf(line.split(",")[7]), line);
//            }
//        });
//
//        JavaPairRDD<Integer, Tuple2<String, String>> joinRDD = empFilterMapRDD.join(deptFilterMapRDD);
//
//        joinRDD.foreach(new VoidFunction<Tuple2<Integer, Tuple2<String, String>>>() {
//
//            public void call(Tuple2<Integer, Tuple2<String, String>> t) throws Exception {
//                // TODO Auto-generated method stub
//
//                System.out.println(
//
//                        t._2._1.split(",")[0] + "\t" +
//                                t._2._1.split(",")[1] + "\t" +
//                                t._2._2.split(",")[0] + "\t" +
//                                t._2._2.split(",")[1] + "\t" +
//                                t._2._1.split(",")[5]
//
//                );
//
//            }
//        });
//
//        sc.close();
//
//    }
//
//    /**
//     * RDD模拟以下SQL实现：
//     * <p>
//     * select e.empno,e.ename,d.deptno,d.dname,e.sal
//     * from emp e left join dept d on e.deptno = d.deptno
//     * ;
//     */
//
//    @SuppressWarnings("serial")
//    public static void selectLeftJoin() {
//
//        SparkConf conf = new SparkConf().setAppName("selectLeftJoin").setMaster("local");
//        JavaSparkContext sc = new JavaSparkContext(conf);
//
//        JavaRDD<String> deptRDD = sc.textFile("files//dept.csv");
//        JavaRDD<String> empRDD = sc.textFile("files//emp.csv");
//
//        JavaPairRDD<Integer, String> deptMapRDD = deptRDD.mapToPair(new PairFunction<String, Integer, String>() {
//            public Tuple2<Integer, String> call(String line) throws Exception {
//                // TODO Auto-generated method stub
//                return new Tuple2<Integer, String>(Integer.valueOf(line.split(",")[0]), line);
//            }
//        });
//
//        JavaPairRDD<Integer, String> empMapRDD = empRDD.mapToPair(new PairFunction<String, Integer, String>() {
//            public Tuple2<Integer, String> call(String line) throws Exception {
//                // TODO Auto-generated method stub
//                return new Tuple2<Integer, String>(Integer.valueOf(line.split(",")[7]), line);
//            }
//        });
//
//        JavaPairRDD<Integer, Tuple2<String, Optional<String>>> joinRDD = empMapRDD.leftOuterJoin(deptMapRDD);
//
//        joinRDD.foreach(new VoidFunction<Tuple2<Integer, Tuple2<String, Optional<String>>>>() {
//
//            public void call(Tuple2<Integer, Tuple2<String, Optional<String>>> t) throws Exception {
//                // TODO Auto-generated method stub
//
//                System.out.println(
//
//                        t._2._1.split(",")[0] + "\t" +
//                                t._2._1.split(",")[1] + "\t" +
//                                t._2._2.get().split(",")[0] + "\t" +
//                                t._2._2.get().split(",")[1] + "\t" +
//                                t._2._1.split(",")[5]
//
//                );
//
//            }
//        });
//
//        sc.close();
//
//    }
//
//}
