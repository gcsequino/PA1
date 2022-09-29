import org.apache.hadoop.conf.Configuration; 
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable; 
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configured;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.StringReader;
import java.io.IOException;
import java.util.ArrayList;

public class TwoB extends Configured implements Tool{

    public static class WordCountMapper extends Mapper<LongWritable, Text, Text, Text>{

        static String[] author;
        static String[] date;
        static String book_id;

        public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException{

            String curr_line = value.toString();

            // if we are in the header
            if(curr_line.contains("<~~~>")){
                try{
                    String[] curr_line_arr = curr_line.split("<~~~>");
                    author = curr_line_arr[0].toString().split(" ");
                    date = curr_line_arr[1].toString().split(" ");
                    book_id = curr_line;
                } catch(ArrayIndexOutOfBoundsException e) {
                    e.printStackTrace();
                    return;
                }
                return;
            }
            
            // format content
            curr_line = curr_line.toLowerCase()
                                 .replaceAll("[^A-Za-z0-9\\s]", "")
                                 .replaceAll("\\s{2,}", " ")
                                 .trim();
            String[] tokens = curr_line.split(" ");

            String kv_pairs;           
            for(int i = 0; i <= tokens.length; i++) {
                if(i == 0) { // start of the line
                    if(tokens[i].length() >= 1){
                        kv_pairs = "_START_" + "," + tokens[i] + "\t" + author[date.length-1];
                        context.write(new Text(kv_pairs), new Text(book_id));
                    }
                }
                else if(i == tokens.length) { // end of the line
                    if(tokens[i-1].length() >= 1) {
						kv_pairs = tokens[i-1] + "," + "_END_" + "\t" + author[date.length -1];
						context.write(new Text(kv_pairs), new Text(book_id));
					}
                }
                else { // middle of the line
                    if(tokens[i].length() >= 1){
						kv_pairs = tokens[i-1] + "," + tokens[i] + "\t" + author[date.length -1];
						context.write(new Text(kv_pairs), new Text(book_id));
					}
                }
            }

            //context.write(new Text("a"), new Text("one"));
        }
    }

    public static class WordCountReducer extends Reducer<Text,Text,Text,IntWritable>{
        public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
            // Total count for kv_pair appearences
            ArrayList<String> book_ids = new ArrayList<>();
            for(Text val : values) {
                String str = val.toString();
                book_ids.add(str);
            }

            // How many books does this kv_pair appear in?
            ArrayList<String> unique_id = new ArrayList<>();
            for(String id : book_ids) {
                if (!unique_id.contains(id)) {
                    unique_id.add(id);
                }
            }
            int books = unique_id.size();

            String new_key = key.toString() + "\t" + books;
            context.write(new Text(new_key), new IntWritable(book_ids.size()));
        }
    }

    public static void main(String[] args) throws Exception { 
		//ToolRunner allows for command line configuration parameters - suitable for shifting between local job and yarn
		// example command: hadoop jar <path_to_jar.jar> <main_class> -D param=value <input_path> <output_path>
		//We use -D mapreduce.framework.name=<value> where <value>=local means the job is run locally and <value>=yarn means using YARN 
		int res = ToolRunner.run(new Configuration(), new TwoB(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf, "unigrams author"); 

        job.setJarByClass(TwoB.class); 
        job.setMapperClass(TwoB.WordCountMapper.class); 
        job.setReducerClass(TwoB.WordCountReducer.class);

        job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }
}