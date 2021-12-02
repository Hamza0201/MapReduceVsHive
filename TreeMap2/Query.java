package TreeMap2;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class Query {

  public static class TokenizerMapper extends Mapper<Object, Text, Text, DoubleWritable>{

    private Text storeName = new Text();
    private DoubleWritable netProfit = new DoubleWritable();
    private int startDate;
    private int endDate;

    @Override
    public void setup(Context context) throws IOException, InterruptedException { 
  
    Configuration conf = context.getConfiguration(); 
  
    String param1 = conf.get("s"); 
    String param2 = conf.get("e"); 
  
    startDate = Integer.parseInt(param1); 
    endDate = Integer.parseInt(param2);

    } 

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

      StringTokenizer itr = new StringTokenizer(value.toString());

      while (itr.hasMoreTokens()) {
        
        String temp = itr.nextToken();
        String[] tokens = temp.split("\\|", -1);
        
        if (!tokens[0].equals("")) {
        
          if ((startDate <= Integer.parseInt(tokens[0])) && (Integer.parseInt(tokens[0]) <= endDate)){
        
            if (!tokens[22].equals("")) {

              Double profit = Double.parseDouble(tokens[22]);

              if (!tokens[7].equals("")){
        
                String store_name = tokens[7];
                storeName.set(store_name);
                netProfit.set(profit);
                context.write(storeName, netProfit);
              
              }
            }
          }
        }
      }
    }
  }

  public static class IntSumReducer extends Reducer<Text, DoubleWritable ,Text, DoubleWritable> {

    private DoubleWritable result = new DoubleWritable();

    public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {

      Double total = 0.00;

      for (DoubleWritable val : values) {
        total += val.get();
      }

      result.set(total);

      context.write(key, result);
    }

  }

  public static class JoinMapper1 extends Mapper<Object, Text, IntWritable, Text>{

    private IntWritable word = new IntWritable();
    private Text profit_word = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

      StringTokenizer itr = new StringTokenizer(value.toString(), "\n");

      while (itr.hasMoreTokens()) {

        String temp = itr.nextToken();
        String[] tokens = temp.split("\\s+");

        Double profit = Double.parseDouble(tokens[1]);
        String store_name = tokens[0].toString();

        word.set(Integer.parseInt(store_name));
        profit_word.set("id   " + profit.toString());

        context.write(word, profit_word);
      }
    }

  }

  public static class JoinMapper2 extends Mapper<Object, Text, IntWritable, Text>{

    private IntWritable word = new IntWritable();
    private Text profit_word = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

      StringTokenizer itr = new StringTokenizer(value.toString(), "\n");

      while (itr.hasMoreTokens()) {

          String temp = itr.nextToken();
          String[] tokens = temp.split("\\|", -1);

          if(!tokens[6].equals("")){

            Integer profit = Integer.parseInt(tokens[6]);

            if (!tokens[0].equals("")){

              String store_name = tokens[0];

              word.set(Integer.parseInt(store_name));
              profit_word.set("emp   " + profit.toString());

              context.write(word, profit_word);
            }

          }
      }
    }

  }

  public static class FinalReducer extends Reducer<IntWritable, Text ,IntWritable, Text> {

    private int number;
    private int count;
    private Text result = new Text();

    @Override
    public void setup(Context context) throws IOException, InterruptedException { 
  
      Configuration conf = context.getConfiguration(); 

      String param1 = conf.get("N"); 

      number = Integer.parseInt(param1); 

      count = 0;

    } 

    public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

      Integer total = 0;
      Double sum = 0.00;

      for (Text val : values) {

        String parts[] = val.toString().split("   ");

        if (parts[0].equals("emp")) {

          total += Integer.parseInt(parts[1]);

        } else if (parts[0].equals("id")) {

            sum += Double.parseDouble(parts[1]);

        }

      }

      sum = Math.round(sum * 100.0) /100.0;
      int nmber = 16 - String.format("%.2f", sum).length();
      String repeated = new String(new char[nmber]).replace("\0", " "); 
      String str = String.format("|    %.2f " + repeated + "|  %d", sum, total);

      result.set(str);

      if (count < number) { 
        if (!sum.equals(0.00)){
          context.write(key, result); 
          count++; 
        }
      }

    }

  }

  public static void main(String[] args) throws Exception {

    Configuration conf = new Configuration();
    conf.set("s", args[1]); 
    conf.set("e", args[2]); 
    conf.set("mapred.conpress.map.output", "true");
    conf.set("mapred.map.output.compression.codec", 
    "org.apache.hadoop.io.compress.LzoCodec");
    Path out = new Path(args[5]);
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(Query.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DoubleWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[3]));
    FileOutputFormat.setOutputPath(job, new Path(out, "out1"));
    job.waitForCompletion(true);

    Configuration conf2 = new Configuration();
    conf2.set("N", args[0]);
    conf2.set("mapred.conpress.map.output", "true");
    conf2.set("mapred.map.output.compression.codec", 
    "org.apache.hadoop.io.compress.LzoCodec");
    Job job2 = Job.getInstance(conf2, "final");
    job2.setJarByClass(Query.class);
    job2.setCombinerClass(FinalReducer.class);
    job2.setReducerClass(FinalReducer.class);
    job2.setOutputKeyClass(IntWritable.class);
    job2.setOutputValueClass(Text.class);

    MultipleInputs.addInputPath(job2, new Path(out, "out1"), TextInputFormat.class, JoinMapper1.class);
    MultipleInputs.addInputPath(job2, new Path(args[4]), TextInputFormat.class, JoinMapper2.class);

    FileOutputFormat.setOutputPath(job2, new Path(out, "out2"));
    System.exit(job2.waitForCompletion(true) ? 0 : 1);
  }

}