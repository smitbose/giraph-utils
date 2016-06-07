package io;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.io.EdgeReader;
import org.apache.giraph.io.formats.GiraphTextInputFormat;
import org.apache.giraph.utils.IntPair;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class SNAPReader extends EdgeReader<IntWritable, NullWritable>{

	/*
	 * Internal line Record Reader
	 */
	private RecordReader<LongWritable, Text> lineRecordReader;
	/*
	 * Context passed to initialize
	 */
	private TaskAttemptContext context;
	/*
	 * Text input format
	 */
	private GiraphTextInputFormat textInputFormat;
	/*
	 * Pattern to break the line, that breaks the line based on tabs and spaces
	 */
	private Pattern SEP = Pattern.compile("[\t ]");
	/*
	 * processed line to hold the tokenised values
	 */
	private IntPair processedline;
	/**
	 * Parameterized constructor
	 * @param textInputFormat Initialises the input format
	 */
	public SNAPReader(GiraphTextInputFormat textInputFormat)
	{
		this.textInputFormat = textInputFormat;
	}
	
	@Override
	public void close() throws IOException {
		
		
		lineRecordReader.close();
	}

	@Override
	public Edge<IntWritable, NullWritable> getCurrentEdge() throws IOException,
			InterruptedException {
		
		processCurrentLine();
		IntWritable targetVertexId = getTargetVertexId(processedline);
		return EdgeFactory.create(targetVertexId);
	}

	private IntWritable getTargetVertexId(IntPair processedline2) {
		return new IntWritable(processedline2.getSecond());
	}
	@Override
	public IntWritable getCurrentSourceId() throws IOException,
			InterruptedException {
		processCurrentLine();
		return getSourceVertexId(processedline);
	}

	private IntWritable getSourceVertexId(IntPair processedline2) {
		// TODO Return the first token
		return new IntWritable(processedline2.getFirst());
	}
	@Override
	public float getProgress() throws IOException, InterruptedException {
		return lineRecordReader.getProgress();
	}

	@Override
	public void initialize(InputSplit inputSplit, TaskAttemptContext context)
			throws IOException, InterruptedException {
	
		this.context = context;
		lineRecordReader = createLineRecordReader(inputSplit, context);
		lineRecordReader.initialize(inputSplit, context);
	}

	@Override
	public boolean nextEdge() throws IOException, InterruptedException {
		
		return lineRecordReader.nextKeyValue();
	}

	protected RecordReader<LongWritable, Text> createLineRecordReader(InputSplit inputSplit, TaskAttemptContext context)
	throws IOException, InterruptedException
	{
		return this.textInputFormat.createRecordReader(inputSplit, context);
	}
	
	protected RecordReader<LongWritable, Text> getLineRecordReader() {
		return lineRecordReader;
	}
	
	protected TaskAttemptContext getContext() {
		return context;
	}
	
	private void processCurrentLine() throws IOException, InterruptedException
	{
		Text line = lineRecordReader.getCurrentValue();
		if(line.charAt(0) == '#' && lineRecordReader.nextKeyValue()) //ignoring comments in dataset
		{
			line = lineRecordReader.getCurrentValue();
		}
		processedline = preprocessLine(line);
	}
	
	private IntPair preprocessLine(Text line) {
		String token[] = SEP.split(line.toString());
		return new IntPair(Integer.valueOf(token[0]),Integer.valueOf(token[1]));
	}
}
