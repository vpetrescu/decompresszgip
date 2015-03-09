
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class DecompressGZ {

  public static class DecompressMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    public void map(Object key, Text value, Context context
            ) throws IOException, InterruptedException {
    	//TODO iterate here over keys
    	// full path to file.gz	
    	String uri = value.toString();
    	Configuration conf = new Configuration();
    	FileSystem fs = FileSystem.get(URI.create(uri), conf);
    	Path inputPath = new Path(uri);
    	CompressionCodecFactory factory = new CompressionCodecFactory(conf);
    	// the correct codec will be discovered by the extension of the file
    	CompressionCodec codec = factory.getCodec(inputPath);

    	if (codec == null) {
    		System.err.println("No codec found for " + uri);
    		System.exit(1);
    	}

    	// remove the .gz extension
        File userFile = new File(uri);
        String basename = userFile.getName();
        Path workpath = FileOutputFormat.getOutputPath(context);
        Path pathoutputUri= new Path(workpath, basename);
        
        InputStream in = null;
        OutputStream out = null;
        try {
        	
          System.out.printf("Input path is %s, %s\n", uri, basename);
          in = codec.createInputStream(fs.open(inputPath));
          out = fs.create(pathoutputUri);
          IOUtils.copyBytes(in, out, conf);
        } finally {
          
          in.close();
          out.close();
        }
     }
     // Alternatively use org.apache.commons.io.FilenameUtils 
     static String stripExtension (String str) {
        // Handle null case specially.
        if (str == null) return null;
        // Get position of last '.'.
        int pos = str.lastIndexOf(".");
        // If there wasn't any '.' just return the string as is.
        if (pos == -1) return str;
        // Otherwise return the string, up to the dot.
        return str.substring(0, pos);
     }
  }


  public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.out.printf("Two parameters are required - <input dir> <output dir>\n");
		}
 
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "decompress");
    job.setJarByClass(DecompressGZ.class);
    job.setMapperClass(DecompressMapper.class);
    // process every line
    job.setInputFormatClass(NLineInputFormat.class);
	NLineInputFormat.addInputPath(job, new Path(args[0]));
	job.getConfiguration().setInt(
			"mapreduce.input.lineinputformat.linespermap", 1);
	
    job.setNumReduceTasks(0);
    LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.setInputPaths(job, new Path("/Users/alex/compressed_files_from_original/*.txt"));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
