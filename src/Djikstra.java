import java.io.Serializable;
import java.util.List;
import java.util.Set;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.lang.Boolean;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.AbstractJavaRDDLike;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.PairFunction;

import com.google.common.collect.Iterables;

import scala.*;

public class Djikstra {
	public static void main(String [] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("Shortest distance");
		JavaSparkContext context = new JavaSparkContext(conf);

		JavaRDD<String> inputs = context.textFile(args[1]);
		JavaPairRDD<String, Tuple2<String, Integer>> adj1 = inputs.mapToPair(x -> new Tuple2<String, Tuple2<String, Integer>>(x.split(",")[0], new Tuple2<String, Integer>(x.split(",")[1], Integer.parseInt(x.split(",")[2]))));

		List<Tuple2<String, Tuple2<String, Integer>>> temp1 = new ArrayList<Tuple2<String, Tuple2<String, Integer>>>();
		temp1.add(new Tuple2<String, Tuple2<String, Integer>>(args[0], new Tuple2<String, Integer>(args[0], 0)));
		JavaPairRDD<String, Tuple2<String, Integer>> init = context.parallelizePairs(temp1);

		JavaPairRDD<String, Tuple2<String, Integer>> temp10 = inputs.mapToPair(x -> new Tuple2<String, Tuple2<String, Integer>>(x.split(",")[0], new Tuple2<String, Integer>("-", Integer.MAX_VALUE))).distinct();
		JavaPairRDD<String, Tuple2<String, Integer>> temp11 = inputs.mapToPair(x -> new Tuple2<String, Tuple2<String, Integer>>(x.split(",")[1], new Tuple2<String, Integer>("-", Integer.MAX_VALUE))).distinct();
		JavaPairRDD<String, Tuple2<String, Integer>> temp12 = temp11.subtract(temp10);
		JavaPairRDD<String, Tuple2<String, Integer>> adj = adj1.union(temp12);

		JavaPairRDD<String, Iterable<Tuple2<String, Integer>>> temp3 = adj.groupByKey();
		JavaPairRDD<String, Tuple2<String, Integer>> result = temp3.mapToPair(x -> new Tuple2<String, Tuple2<String, Integer>>(x._1, new Tuple2<String, Integer>("-", Integer.MAX_VALUE)));

		while (true) {
			JavaPairRDD<String, Tuple2<Tuple2<String, Integer>, Tuple2<String, Integer>>> temp4 = init.join(adj).filter(x -> !(x._2._1._1.contains(x._2._2._1)));
			JavaPairRDD<String, Tuple2<String, Integer>> temp5 = temp4.mapValues(x -> new Tuple2<String, Integer>(x._1._1 + "-" + x._2._1, x._1._2 + x._2._2));
			JavaPairRDD<String, Tuple2<String, Integer>> temp6 = temp5.mapToPair(x -> new Tuple2<String, Tuple2<String, Integer>>(x._2._1.split("-")[x._2._1.split("-").length - 1], new Tuple2<String, Integer>(x._2._1, x._2._2)));
			JavaPairRDD<String, Tuple2<Iterable<Tuple2<String, Integer>>, Iterable<Tuple2<String, Integer>>>> temp7 = result.cogroup(temp6);

			result = temp7.mapToPair(new PairFunction<Tuple2<String, Tuple2<Iterable<Tuple2<String, Integer>>, Iterable<Tuple2<String, Integer>>>>, String, Tuple2<String, Integer>>() {
				@Override
				public Tuple2<String, Tuple2<String, Integer>> call(Tuple2<String, Tuple2<Iterable<Tuple2<String, Integer>>, Iterable<Tuple2<String, Integer>>>> arg0) throws Exception {
					Tuple2<String, Integer> x = arg0._2._1.iterator().next();

					int ctr = 0;
					for (Object i : arg0._2._2) {
						ctr++;
					}

					if (ctr > 0) {
						Iterator<Tuple2<String, Integer>> y = arg0._2._2.iterator();
						Tuple2<String, Integer> mini = y.next();
						String z = mini._1.split("-")[mini._1.split("-").length - 1];

						while (y.hasNext()) {
							Tuple2<String, Integer> t = y.next();

							if (t._2 < mini._2) {
								z = t._1.split("-")[t._1.split("-").length - 1];
								mini = t;
							}
						}

						if (x._2 >= mini._2) {
							return new Tuple2<String, Tuple2<String, Integer>>(z, mini);
						}
					}

					return new Tuple2<String, Tuple2<String, Integer>>(arg0._1, x);
				}
			});

			init = temp4.mapToPair(x -> new Tuple2<String, Tuple2<String, Integer>>(x._2._2._1, new Tuple2<String, Integer>(x._2._1._1 + "-" + x._2._2._1, x._2._1._2 + x._2._2._2)));

			if (temp7.filter(x -> x._2._2.iterator().hasNext()).count() == 0) {
				// for storing sorted in file
				JavaPairRDD<Integer, Tuple2<String, String>> sorted_list1 = result.mapToPair(x -> new Tuple2<Integer, Tuple2<String, String>>(x._2._2, new Tuple2<String, String>(x._1, x._2._1)));
				JavaPairRDD<Integer, Tuple2<String, String>> sorted_list2 = sorted_list1.sortByKey();
				JavaPairRDD<Integer, Tuple2<String, String>> sorted_list3 = sorted_list2.filter(x -> !(x._2._1.equals(args[0])));

				JavaRDD<String> temp13 = sorted_list3.map(new Function<Tuple2<Integer, Tuple2<String, String>>, String>() {
					public String call(Tuple2<Integer, Tuple2<String, String>> arg0) throws Exception {
						if (arg0._1 == Integer.MAX_VALUE || arg0._2._2.contains("--") || arg0._2._2.equals("-")) {
							return arg0._2._1 + ",-1,";
						}

						return arg0._2._1 + "," + arg0._1 + "," + arg0._2._2;
					}
				});

				temp13.repartition(1).saveAsTextFile(args[2]);
				break;
			}
		}
	}
}
