
/** 
 * DecompressGZ takes as input a folder with .gz files 
 * and decompresses them in the provided output folder using map reduce.
 * 
 * @author  Viviana Petrescu
 **/
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.PriorityQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


	 
public class DecompressGZ extends Configured implements Tool {

  public static class DecompressMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    public void map(Object key, Text value, Context context
            ) throws IOException, InterruptedException {
    	// The value represents the full path to a compressed <filename>.gz
    	String uri = value.toString();
    	// Some line records are marked with NONE and need to be skipped.
    	if (uri.equals("NONE") == false) {
    		Configuration conf = new Configuration();
    		FileSystem fs = FileSystem.get(URI.create(uri), conf);
    		Path inputPath = new Path(uri);
    		
    		// We let the compression factory detect the codec,
    		// but in our case it should be GZIP
    		CompressionCodecFactory factory = new CompressionCodecFactory(conf);
    		CompressionCodec codec = factory.getCodec(inputPath);
    		if (codec == null) {
    			System.err.println("No codec found for " + uri);
    			System.exit(1);
    		}

    		// Remove the .gz extension.
    		File userFile = new File(uri);
    		String basename = stripExtension(userFile.getName());
    		// The side effect files placed here will be moved to the correct output 
    		// directory.
    		Path workpath = FileOutputFormat.getOutputPath(context);
    		Path pathoutputUri= new Path(workpath, basename);
        
    		// IO Streams for reading the compressed file and
    		// writing the uncompressed one.
    		InputStream in = null;
    		OutputStream out = null;
    		if (fs.exists(inputPath)) { // Test that the compressed file exists.
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
    	}
     }
   
     // Code for removing the extension of a file (from stackoverflow)
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

  public static class FileGroup {
	  //  The sum of the sizes of the files in this group
	  public long group_file_size;
	  //  The full path of the filenames in this group
	  public ArrayList<String> filenames;
	  
	  // Constructor for creating a FileGroup from 
	  // merging two FileGroups
	  FileGroup(FileGroup fg1, FileGroup fg2) {
		  group_file_size = fg1.group_file_size + fg2.group_file_size;
		  filenames = new ArrayList<String>();
		  filenames.addAll(fg1.filenames);
		  filenames.addAll(fg2.filenames);
	  }
	  FileGroup(long fsize, String filename) {
		  group_file_size = fsize;
		  filenames = new ArrayList<String>();
		  filenames.add(filename);
	  }
  }
  public static int computeComplexFilesStructure(String input_directory, 
				             Configuration conf) throws IOException, InterruptedException {
	  // List all files in the input directory
	  FileSystem fs = FileSystem.get(URI.create(input_directory), conf);
	  FileStatus[] status = fs.listStatus(new Path(input_directory));
	  Path[] listedPaths = FileUtil.stat2Paths(status);

	  // Create a union file structure with size ->filename
	   PriorityQueue<FileGroup> fileGroupQueue = new PriorityQueue<FileGroup>(listedPaths.length, new Comparator<FileGroup>() {
	        public int compare(FileGroup fg1, FileGroup fg2) {
	            return (int) (fg1.group_file_size - fg2.group_file_size);
	        }
	    });
	   for (int i = 0; i < listedPaths.length; i++)  {
			  fileGroupQueue.add(new FileGroup(status[i].getLen(), listedPaths[i].toUri().toString()));
	   }
	   
	   // Merge two FileGroups until the queue size is <=50, equivalent with 50 map tasks
	   while (fileGroupQueue.size() > 50 && fileGroupQueue.size() > 2) {
		   FileGroup fg1 = fileGroupQueue.poll();
		   FileGroup fg2 = fileGroupQueue.poll();
		   fileGroupQueue.add(new FileGroup(fg1, fg2));
	   }
	   PriorityQueue<FileGroup> saved_fileGroupQueue = new PriorityQueue<FileGroup>(fileGroupQueue);
	   
	   // Determine the number of lines to be processed by a mapper.
	   int nbrlines = 0;
	   while (fileGroupQueue.size() > 0) {
		   FileGroup fg1 = fileGroupQueue.poll();
		   if (fg1.filenames.size() > nbrlines)
			   nbrlines = fg1.filenames.size();
	   }
	   // If the temporary output we need to write to exists already, delete it.
	   Path outFile = new Path("/tmp/input.temporary");
	   if (fs.exists(outFile)) {
			  System.out.println("File exitsts");
			  fs.delete(outFile, true);
	   }
	   FSDataOutputStream out = fs.create(outFile);
	   while (saved_fileGroupQueue.size() > 0) {
		   FileGroup fg1 = saved_fileGroupQueue.poll();
		   for (int i = 0; i < fg1.filenames.size(); ++i) {
			   out.writeBytes(fg1.filenames.get(i));
			   out.writeBytes("\n");
		   }
		   // Add NONE for the remaining filenames
		   for (int i = fg1.filenames.size(); i < nbrlines; ++i) {
			   out.writeBytes("NONE\n");
		   }
	   }
	   
	  out.close();
	  fs.close();

	  return nbrlines;
  }
  
  @Override
  public int run(String[] args) throws Exception {
	if (args.length != 2) {
			System.out.printf("Two parameters required - <input dir> <output dir>\n");
	}

    Configuration conf = this.getConf();
    // Do not create _SUCCESS file
    conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false");
	int lines_per_map = computeComplexFilesStructure(args[0], conf);

	System.out.printf("Max nbr files processed per map %d\n", lines_per_map);
    Job job = Job.getInstance(conf, "decompress");
    job.setJarByClass(DecompressGZ.class);
    job.setMapperClass(DecompressMapper.class);
    job.setInputFormatClass(NLineInputFormat.class);
	NLineInputFormat.addInputPath(job, new Path(args[0]));
	job.getConfiguration().setInt(
			"mapreduce.input.lineinputformat.linespermap", lines_per_map);
	
    job.setNumReduceTasks(0);
    // Ensures that part output files are created only if there is output data
    LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
  
    FileInputFormat.setInputPaths(job, new Path("/tmp/input.temporary"));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
   
    // Delete temporary file after the task has finished.
    Path outFile = new Path("/tmp/input.temporary");
	FileSystem fs = FileSystem.get(URI.create(args[0]), conf);
	if (fs.exists(outFile)) {
		  System.out.println("File exists, deleting");
		  fs.deleteOnExit(outFile);
	}
 
    return job.waitForCompletion(true) ? 0 : 1;
  }
  
  public static void main(String[] args) throws Exception {
      int res = ToolRunner.run(new Configuration(), new DecompressGZ(), args);
      System.exit(res);
  }
}

