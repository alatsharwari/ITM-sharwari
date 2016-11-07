// cc MaxTemperatureWithCompression Application to run the maximum temperature job producing compressed output
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.compress.BZip2Codec;

//vv MaxTemperatureWithCompression
public class MaxTemperatureWithCompression {

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: MaxTemperatureWithCompression <input path> " +
        "<output path>");
      System.exit(-1);
    }

    Job job = new Job();
    job.setJarByClass(MaxTemperature.class);
    String ext[] = args[0].split("\\.");
    String extension = ext[2];
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
     /*[*/FileOutputFormat.setCompressOutput(job, true);
    if(extension.equals("bz2"))
    {
        FileOutputFormat.setOutputCompressorClass(job, BZip2Codec.class);/*]*/
       
  }
    else if(extension.equals("gz"))
    {
         FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);/*]*/
    }
    else
   {
    //error
   }

    //FileOutputFormat.setCompressOutput(job, true);
 //   FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);/*]*/
   
   // FileOutputFormat.setOutputCompressorClass(job, BZip2Codec.class);/*]*/
    job.setMapperClass(MaxTemperatureMapper.class);
    job.setCombinerClass(MaxTemperatureReducer.class);
    job.setReducerClass(MaxTemperatureReducer.class);
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
//^^ MaxTemperatureWithCompression
