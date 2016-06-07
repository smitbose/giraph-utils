package io;

import java.io.IOException;
import java.util.List;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.io.EdgeInputFormat;
import org.apache.giraph.io.EdgeReader;
import org.apache.giraph.io.formats.GiraphTextInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * This class reads datasets directly available in SNAP
 * @author soumit
 *
 */
@SuppressWarnings("rawtypes")
public class SNAPInputFormat extends EdgeInputFormat<IntWritable, NullWritable>{

	
	/*
	 * underlying giraph input format
	 */
	protected GiraphTextInputFormat textInputFormat = new GiraphTextInputFormat();
	
	@Override
	public EdgeReader<IntWritable, NullWritable> createEdgeReader(
			InputSplit split, TaskAttemptContext arg1) throws IOException {
		
		return new SNAPReader(textInputFormat);
	}

	@Override
	public void checkInputSpecs(Configuration arg0) { } //nothing to do here

	@Override
	public List<InputSplit> getSplits(JobContext context, int minInputSplitsHint)
			throws IOException, InterruptedException {
		
		return textInputFormat.getEdgeSplits(context);
		
	}
	
	
		
}
