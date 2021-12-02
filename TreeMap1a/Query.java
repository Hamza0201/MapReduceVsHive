package TreeMap1a;

import java.util.*;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Query {

  // Parse Text File to retrieve Key,Value Pairs where the Keys are store names. i.e. ss_store_sk_1.
  // The values are the list of profits associated with the corresponding store name.
  public static class TokenizerMapper extends Mapper<Object, Text, Text, DoubleWritable>{

    private Text storeName = new Text();
    private DoubleWritable netProfit = new DoubleWritable();
    private int startDate;
    private int endDate;

    @Override
    public void setup(Context context) throws IOException, InterruptedException { 
  
      Configuration conf = context.getConfiguration(); 
    
      // Get parameters passed in from command line for the starting date, s and the ending date, e.
      String param1 = conf.get("s"); 
      String param2 = conf.get("e"); 
    
      startDate = Integer.parseInt(param1); 
      endDate = Integer.parseInt(param2);

    } 

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

      StringTokenizer itr = new StringTokenizer(value.toString());

      //Iterate through every record in the file.
      while (itr.hasMoreTokens()) {
        
        String temp = itr.nextToken();
        // Split the records at the pipes and create an array of the data.
        String[] tokens = temp.split("\\|", -1);
        
        // Check Whether record is empty before proceeding to process data.
        if (!tokens[0].equals("")) {
        
          // Only retrieve records where date is between the two dates passed as parameteres in the command line, s and e.
          if ((startDate <= Integer.parseInt(tokens[0])) && (Integer.parseInt(tokens[0]) <= endDate)){
        
            if (!tokens[22].equals("")){
        
              // Get the net profit for the record.
              Double profit = Double.parseDouble(tokens[22]);
        
              if (!tokens[7].equals("")){
                
                // Get the store name for the record.
                String store_name = tokens[7];

                // Convert primitive Java Types to MapReduce Types.
                storeName.set(store_name);
                netProfit.set(profit);

                // Write store name and corresponding net profit to reducer.
                context.write(storeName, netProfit);
              
              }
            }
          }
        }
      }
    }
  }

  // For every key, SUM the array of net profits over the dates to achieve one net profit for every key.
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

  // A TreeMap will be used to sort the Key, Value Pairs by the Value (NetProfit).
  public static class KeyValueSwapper extends Mapper<Object, Text, Text, DoubleWritable>{

    private TreeMap<Double, List<String>> tmap;
    private int topK;
    private int counter;

    // Retrieve the K argument from the command line used to only get the Top-K queries.
    @Override
    public void setup(Context context) throws IOException, InterruptedException { 
      tmap = new TreeMap<Double, List<String>>(); 

      Configuration conf = context.getConfiguration(); 
  
      String param1 = conf.get("N"); 
      
      topK = Integer.parseInt(param1); 

      counter = 0;

    }

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      
      StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
      
      while (itr.hasMoreTokens()) {
      
        String temp = itr.nextToken();
        String [] kv = temp.toString().split("\\s+");
        
        Double deg = Double.parseDouble(kv[1]);

        // Round NetProfit to 2dp for consistency.
        deg = Math.round(deg * 100.0) /100.0;

        deg = (-1) * deg;

        //Check if key exists, if it does then add value to array.
        if (tmap.containsKey(deg)) {
          List<String> old_array= tmap.get(deg);
          old_array.add(kv[0]);
          tmap.put(deg, old_array);
        }
        else{
          // Put Key-Value Pair into the TreeMap where Net Profit is the key to be sorted.
          List<String> old_array = new ArrayList<>(Arrays.asList(kv[0]));
          tmap.put(deg, old_array);
        }
      
      }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException { 
      for (Map.Entry<Double, List<String>> entry : tmap.entrySet()) {

        // Retrieve Top-K Key-Value Pairs from the TreeMap.
        Double count = entry.getKey(); 
        count = (-1) * count;

        for (int i = 0; i < entry.getValue().size(); i++) {
          
          Integer name = Integer.parseInt(entry.getValue().get(i)); 

          // Format Output with Pipes.
          int number = 11 - String.format("%d", name).length();
          String repeatedSpaces = new String(new char[number]).replace("\0", " ");
          String repeatedSpaces2 = new String(new char[number-2]).replace("\0", " "); 
          String str = String.format("ss_sold_date_sk_%d" + repeatedSpaces + "|" + repeatedSpaces2, name);

          //Only Retrieve Top-K rows.
          if (counter < topK) {
            counter++;
            context.write(new Text(str), new DoubleWritable(count));
          } 

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
    Path out = new Path(args[4]);
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(Query.class);
    job.setMapperClass(TokenizerMapper.class);
    // job.setCombinerClass(IntSumReducer.class);
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
    Job job2 = Job.getInstance(conf2, "frequency");
    job2.setJarByClass(Query.class);
    job2.setMapperClass(KeyValueSwapper.class);
    job2.setNumReduceTasks(0);
    job2.setOutputKeyClass(DoubleWritable.class);
    job2.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job2, new Path(out, "out1"));
    FileOutputFormat.setOutputPath(job2, new Path(out, "out2"));
    System.exit(job2.waitForCompletion(true) ? 0 : 1);

  }
  
}