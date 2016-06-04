import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

public class GiraphHelloWorld extends BasicComputation<LongWritable, DoubleWritable, FloatWritable, DoubleWritable>
{
    @Override
    public void compute(Vertex<LongWritable,DoubleWritable,FloatWritable> vertex, Iterable<DoubleWritable> messages)
    {
        System.out.println("Hello World from the: "+vertex.getId().toString() + " who is following:");
        
        for(Edge<LongWritable, FloatWritable> e: vertex.getEdges())
        {
            System.out.print(" "  + e.getTargetVertexId());
        }
        System.out.println("");

        vertex.voteToHalt();
    }

    
}