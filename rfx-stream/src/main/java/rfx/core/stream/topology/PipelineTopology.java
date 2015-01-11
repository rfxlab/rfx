package rfx.core.stream.topology;

/**
 * PipelineTopology is the topology of a set of data processing elements connected in series, 
 * where the output of one element is the input of the next one. <br>
 * The elements of a pipeline are often executed in parallel or in time-sliced fashion
 * 
 * @author trieu
 *
 */
public abstract class PipelineTopology extends BaseTopology{
	@Override
	public BaseTopology buildTopology() {
		return build();
	}	
	public abstract BaseTopology build();
}