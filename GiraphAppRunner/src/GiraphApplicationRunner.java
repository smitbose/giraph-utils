import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.io.formats.GiraphFileInputFormat;
import org.apache.giraph.io.formats.JsonLongDoubleFloatDoubleVertexInputFormat;
import org.apache.giraph.job.GiraphJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class GiraphApplicationRunner implements Tool 
{
	private Configuration conf;
	private static String inputPath;
	//private static String outputPath;
	
	public String getInputPath() {
		return inputPath;
	}

	public void setInputPath(String inputPath) {
		this.inputPath = inputPath;
	}

	/*public String getOutputPath() {
		return outputPath;
	}

	public void setOutputPath(String outputPath) {
		this.outputPath = outputPath;
	}*/
	
	@Override
	public Configuration getConf() 
	{
		return conf;
	}
	@Override
	public void setConf(Configuration conf) 
	{
		this.conf=conf;
	}

	@SuppressWarnings("static-access")
	@Override
	public int run(String[] args) throws Exception 
	{		
			String	inputfiles="tiny_graph.txt";
			setInputPath("/home/soumit/internship/"+inputfiles);
			GiraphConfiguration giraphConf=new GiraphConfiguration(getConf());
			giraphConf.setComputationClass(GiraphHelloWorld.class);
			giraphConf.setVertexInputFormatClass(JsonLongDoubleFloatDoubleVertexInputFormat.class);
			//giraphConf.setVertexOutputFormatClass(IdWithValueTextOutputFormat.class);
			GiraphFileInputFormat.addVertexInputPath(giraphConf, new Path(getInputPath()));		
			giraphConf.setWorkerConfiguration(0, 1, 100.0f);
			giraphConf.SPLIT_MASTER_WORKER.set(giraphConf, false);
			giraphConf.LOG_LEVEL.set(giraphConf, "error");
			giraphConf.setLocalTestMode(true);
			giraphConf.setMaxNumberOfSupersteps(10000);
			GiraphJob job = new GiraphJob(giraphConf,getClass().getName());
			//setOutputPath("/home/soumit/internship/"+"output/"+inputfiles);
			//FileOutputFormat.setOutputPath(job.getInternalJob(), new Path(getOutputPath()));
			//File directory=new File(getOutputPath());
			//FileUtils.deleteDirectory(directory);
			job.run(true);
		return 1;
	}
	
	public static void main(String args[]) throws Exception
	{
		int t = ToolRunner.run(new GiraphApplicationRunner(),args);
		if (t!=1)
		{System.err.println("Error in setting configuration if giraph");}
	}
	}