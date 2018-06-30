public enum VisualizationType {

	PCA("PCA"), SAMMONS("Sammon's projection");

	private final String name;

	VisualizationType(String name) {
		this.name = name;
	}

	public String toString() {
		return this.name;
	}
}