
/* Powerby Jiuzhang and HzYan

First mapreducer: build n-gram library
Input: Wiki word library, txt file
Ouput: word counts - words pair
Methodolgy: Mapreducer, N-gram model
Date: June 5th 2019

 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class NGramLibraryBuilder {
	public static class NGramMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		int noGram;
		@Override
		public void setup(Context context) {
			//Set parameter "noGram" (Number of gram) according to the command line
			Configuration configuration = context.getConfiguration();
			noGram = configuration.getInt("noGram", 5);
		}

		// Mapper
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// 1. Read documents by line
			String line = value.toString();
			line = line.trim().toLowerCase();//ignore upper case and inappropriate spaces before and after words

			//2. Remove strange characters, replace none alphabet character with space
			line.replaceAll("^[a-z]", " ");

			//3. Separate words by space and stored in String array
			String[] words = line.split("\\s+");
			
			//4. Build n-gram based on array of words
			if (words.length < 2) {
				return; // minimum 2-gram
			}

			for (int i = 0; i < words.length - 1; i++) { // minimum 2-gram
				StringBuilder sb = new StringBuilder();
				for (int j = 1; i + j < words.length && j < noGram; j++) {
					sb.append(words[j]).append(" ");
				}
				context.write(new Text(sb.toString().trim()), new IntWritable(1)); // Write on the hdfs
			}
		}
	}

	public static class NGramReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		// Reducer
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {

			//Sum up the total count for n-gram
			int sum = 0; // default value = 0 if not defined
			for (IntWritable value : values) {
				sum += value.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}

}