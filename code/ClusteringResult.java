import java.util.List;

public class ClusteringResult {

	private List<Integer> clusters;
	private String hiearchicalCluster;
	private int numberOfGroups;
	private VisualizationType visualizationType;

	public ClusteringResult(List<Integer> clusters, String hiearchicalCluster,
			int numberOfGroups, VisualizationType visualizationType) {
		this.clusters = clusters;
		this.hiearchicalCluster = hiearchicalCluster;
		this.numberOfGroups = numberOfGroups;
		this.visualizationType = visualizationType;
	}

	public List<Integer> getClusters() {
		return clusters;
	}

	public String getHiearchicalCluster() {
		return hiearchicalCluster;
	}

	public int getNumberOfGroups() {
		return numberOfGroups;
	}

	public VisualizationType getVisualizationType() {
		return visualizationType;
	}

}