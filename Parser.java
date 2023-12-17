import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

/*
1979   23   23   2   43   24   25   26   26   26   26   25   26  25 
1980   26   27   28  28   28   30   31   31   31   30   30   30  29 
1981   31   32   32  32   33   34   35   36   36   34   34   34  34 
1984   39   38   39  39   39   41   42   43   40   39   38   38  40 
1985   38   39   39  39   39   41   41   41   00   40   39   39  45 

*/

public class Parser {
    // Mapper class
    public static class E_EMapper extends MapReduceBase implements
            Mapper<LongWritable, /* Input key Type */
                    Text, /* Input value Type */
                    Text, /* Output key Type */
                    Text> /* Output value Type */
    {
        // Map function
      static Pattern p = Pattern.compile("([0-9.]*).*/(.*)/.*\"GET (.*)\".*");

      public void map(LongWritable key, Text value, 
      OutputCollector<Text, Text> output,   
      
      Reporter reporter) throws IOException { 
        // Ip_address - - timestamp "request type File" bytes cs
        // 10.223.157.186 - - [15/Jul/2009:15:50:36] "GET RagingPhoenix_2DSleeve.jpeg" 200 168
         String line = value.toString(); 

         Matcher m = p.matcher(line);
         if(m.matches()) {
            String ip = m.group(1);
            String month = m.group(2);
            String file = m.group(3);
            
            Text k = new Text(month + " " + file);
            Text v = new Text(ip);
            output.collect(k, v); 
         }

      }

        // public void map2(LongWritable key, Text value,
        //         OutputCollector<Text, IntWritable> output,

        //         Reporter reporter) throws IOException {
        //     String line = value.toString();
        //     String lasttoken = null;
        //     StringTokenizer s = new StringTokenizer(line, "\t");
        //     String year = s.nextToken();

        //     while (s.hasMoreTokens()) {
        //         lasttoken = s.nextToken();
        //     }
        //     int avgprice = Integer.parseInt(lasttoken);
        //     output.collect(new Text(year), new IntWritable(avgprice));
        // }
    }

    // Reducer class
    public static class E_EReduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
        // month file [ip_address_1 ip_address2 ip_address_3 ...]

        // Reduce function
        public void reduce(Text key, Iterator<Text> values,
                OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            
            StringBuilder sb = new StringBuilder();
            while (values.hasNext()) {
                Text val = values.next();
                sb.append(val.toString()).append(" ");
            }
            output.collect(key, new Text(sb.toString()));
        }

        //         public void reduce(Text key, Iterator<IntWritable> values,
        //         OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
        //     int maxavg = 30;
        //     int val = Integer.MIN_VALUE;

        //     while (values.hasNext()) {
        //         if ((val = values.next().get()) > maxavg) {
        //             output.collect(key, new IntWritable(val));
        //         }
        //     }
        // }
    }

    // Main function
    public static void main(String args[]) throws Exception {
        JobConf conf = new JobConf(Parser.class);

        conf.setJobName("log_parser");
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);
        conf.setMapperClass(E_EMapper.class);
        conf.setCombinerClass(E_EReduce.class);
        conf.setReducerClass(E_EReduce.class);
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);
    }
}
