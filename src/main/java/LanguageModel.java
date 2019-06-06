import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class LanguageModel {
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		int threshold;

		@Override
		public void setup(Context context) {
			// Obtain values for threshold from the command line
			Configuration configuration = context.getConfiguration();
			threshold = configuration.getInt("threshold", 20);
		}

		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// 1. corner cases
			if((value == null) || (value.toString().trim()).length() == 0) {
				return;
			}

			// 2. Ouput key and ouput values
			// The input is from the first Mapreducer: e.g. "this is cool\t20"
			String line = value.toString().trim();
			String[] wordsCount = line.split("\t");
			if(wordsCount.length < 2) {
				return;
			}
			
			String[] words = wordsCount[0].split("\\s+"); //The first part "this is cool"
			int count = Integer.valueOf(wordsCount[1]);  // The second part "20"

			//Filter the n-gram lower than threshold
			if (count < threshold) {
				return;
			}

			//this is --> cool = 20
			StringBuilder sb = new StringBuilder();
			for (int i = 0; i < words.length - 1; i++) {
				sb.append(words[i]).append(" ");
			}
			String outputKey = sb.toString().trim();
			String outputValue = words[words.length - 1] + "=" + count;

			// 3. Write to reducer
			if (!(outputKey == null || outputKey.length() == 0)) {
				context.write(new Text(outputKey), new Text(outputValue));
			}
		}
	}

	public static class Reduce extends Reducer<Text, Text, DBOutputWritable, NullWritable> {

		int K;
		// Get the n parameter from the configuration
		@Override
		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			K = conf.getInt("K", 5);
		}

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			/* PriorityQueue or TreeMap for topK algorithm
			 For PriorityQueue, we need define a Class Pair and a comparator function implements Comparator()
			*/

			// 1. Simpler method: TreeMap
			// Caution: the default treemap is sorted by keys in ascendant order, topK need a reverse order
			TreeMap<Integer, List<String>> treeMap = new TreeMap<Integer, List<String>>(Collections.<Integer>reverseOrder());

			/*
			Input:
				This is <"cool=20", "apple=100" ...>
				values = <"cool=20", "apple=100" ...>
			*/

			// 2. Input into treemap
			for (Text value : values) {
				String currentValue = value.toString().trim();
				String word = currentValue.split("=")[0];
				int count = Integer.parseInt(currentValue.split("=")[1]);
				if (treeMap.containsKey(count)) {
					treeMap.get(count).add(word);
				} else {
					List<String> list = new ArrayList<String>();
					list.add(word);
					treeMap.put(count, list);
				}
			}

			// 3. Output
			/*
			Output target:
				Format: DBOutputWritable - NullWritable
					First: key (String)
					Second: following words (String)
					Third: count (Integer)
			 */
			Iterator<Integer> iterator = treeMap.keySet().iterator();
			for (int i = 0; iterator.hasNext() && i < K;) {
				int count = iterator.next();
				List<String> list = treeMap.get(count);
				for (String word : list) {
					context.write(new DBOutputWritable(key.toString(), word, count), NullWritable.get());
					i++; // Top K = top N
				}
			}

		}
	}
}
