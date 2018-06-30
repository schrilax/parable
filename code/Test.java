import java.io.IOException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

import weka.clusterers.EM;
import weka.clusterers.HierarchicalClusterer;
import weka.core.Attribute;
import weka.core.EuclideanDistance;
import weka.core.FastVector;
import weka.core.Instance;
import weka.core.Instances;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;

public class Test {
	
	//public static double bic(double llh, int numClust, int sampleSize) {
    //    final int k = numClust * 2;
    //    return -2d * llh * sampleSize + k * Math.log(sampleSize);
    //}
	
	//public static int guessNumClusters(EM clusterer, Instances instances, int start, int end) throws Exception {
    //    ClusterEvaluation eval = new ClusterEvaluation();
    //    int bestNum = start;
        
    //    double best = Double.POSITIVE_INFINITY;
    //    double bic;
        
    //    for (int c = start; c <= end; c++) {
    //        clusterer.setNumClusters(c);
    //        clusterer.buildClusterer(instances);
    //        eval.setClusterer(clusterer);
    //        eval.evaluateClusterer(instances);
    //        bic = bic(eval.getLogLikelihood(), c, instances.numInstances());
            
    //        if (bic < best) {
    //           best = bic;
    //            bestNum = c;
    //        }
    //    }
        
    //    System.out.println("Optimal clusters size :" + bestNum);
    //    System.out.println("Optimal Bayesian information criterion :" + best);
        
   //     return bestNum;
   // }

	public static Integer[] test() {
		File file = new File("/Users/suchismit/eclipse/workspace/data.csv");
		FileReader fileReader;
		Integer labels[] = new Integer[150];
		
		try {
			fileReader = new FileReader(file);
			BufferedReader bufferedReader = new BufferedReader(fileReader);			
			
			FastVector attrbName = new FastVector();
			Instances data = new Instances("", attrbName, 1);

			Attribute attrb1 = new Attribute("sepl");
			Attribute attrb2 = new Attribute("sepw");
			Attribute attrb3 = new Attribute("petl");
			Attribute attrb4 = new Attribute("petw");

			attrbName.addElement(attrb1);
			attrbName.addElement(attrb2);
			attrbName.addElement(attrb3);
			attrbName.addElement(attrb4);

			String line;
			while ((line = bufferedReader.readLine()) != null) {
				String[] tokens = line.split(",");
				double[] values = new double[tokens.length];

				for (int iidx = 0; iidx < tokens.length; iidx++) {
					values[iidx] = Double.parseDouble(tokens[iidx]);
				}

				Instance ist = new Instance(1, values);
				data.add(ist);
			}

			EM clusterer = new EM();
			String options[] = {"-N", Integer.toString(3), "-I", Integer.toString(100),};
			
			try {
				clusterer.setOptions(options);
				clusterer.buildClusterer(data);
				int idx = 0;

				Enumeration<?> e = data.enumerateInstances();
				while (e.hasMoreElements()) {
					labels[idx] = clusterer.clusterInstance((Instance) e.nextElement());
					idx++;
				}

				//System.out.println(guessNumClusters(new EM(), actual, 1, 10));
			} catch (Exception e) {
				e.printStackTrace();
			}

			fileReader.close();

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return labels;
	}
	
	public static void main(String[] args) throws IOException, Exception {
		
		int d = 4; // dimensionality
		int p = 3; // partitions
		int k = 3; // clusters in each partition
		
		double[][] clusters = new double[p*k][d];
		Integer counts[] = new Integer[p*k];
		
		for (int ridx = 0; ridx < p*k; ridx++) {		
			counts[ridx] = 0;
			
			for (int cidx = 0; cidx < d; cidx++) {
				clusters[ridx][cidx] = 0.0;
			}
		}
		
		Integer[] labels = test();
		
		Integer labels1[] = new Integer[50];
		Integer labels2[] = new Integer[50];
		Integer labels3[] = new Integer[50];
		
		for (int idx = 0; idx < 50; idx++) {
			labels1[idx] = labels[idx];
			labels2[idx] = labels[idx + 50];
			labels3[idx] = labels[idx + 100];
		}
		
		File file = new File("/Users/suchismit/eclipse/workspace/data.csv");
		FileReader fileReader = null;
		
		try {
			fileReader = new FileReader(file);
			BufferedReader bufferedReader = new BufferedReader(fileReader);

			String line;
			int idx = 0;
			
			while ((line = bufferedReader.readLine()) != null) {
				String[] tokens = line.split(",");

				if (idx < 50) {
					if (labels1[idx] == 0) {
						for (int didx = 0; didx < d; didx++) {
							clusters[0][didx] += Double.parseDouble(tokens[didx]);
						}
						
						counts[0]++;
						
					} else if (labels1[idx] == 1) {
						for (int didx = 0; didx < d; didx++) {
							clusters[1][didx] += Double.parseDouble(tokens[didx]);
						}
						
						counts[1]++;
						
					} else {
						for (int didx = 0; didx < d; didx++) {
							clusters[2][didx] += Double.parseDouble(tokens[didx]);
						}
						
						counts[2]++;
					}
				} else if (idx < 100) {
					if (labels2[idx-50] == 0) {
						for (int didx = 0; didx < d; didx++) {
							clusters[3][didx] += Double.parseDouble(tokens[didx]);
						}
						
						counts[3]++;
						
					} else if (labels2[idx-50] == 1) {
						for (int didx = 0; didx < d; didx++) {
							clusters[4][didx] += Double.parseDouble(tokens[didx]);
						}
						
						counts[4]++;
						
					} else {
						for (int didx = 0; didx < d; didx++) {
							clusters[5][didx] += Double.parseDouble(tokens[didx]);
						}
						
						counts[5]++;
					}
				} else {
					if (labels3[idx-100] == 0) {
						for (int didx = 0; didx < d; didx++) {
							clusters[6][didx] += Double.parseDouble(tokens[didx]);
						}
						
						counts[6]++;
						
					} else if (labels3[idx-100] == 1) {
						for (int didx = 0; didx < d; didx++) {
							clusters[7][didx] += Double.parseDouble(tokens[didx]);
						}
						
						counts[7]++;
						
					} else {
						for (int didx = 0; didx < d; didx++) {
							clusters[8][didx] += Double.parseDouble(tokens[didx]);
						}
						
						counts[8]++;
					}
				}
				
				idx++;
			}
			
			for (int ridx = 0; ridx < p*k; ridx++) {
				int cnt = counts[ridx];
				
				for (int didx = 0; didx < d; didx++) {
					clusters[ridx][didx] = clusters[ridx][didx]/cnt;
				}
			}
			
			double[][] distances = new double[p*k][p*k];
			
			double delta1 = 0.0;
			double delta2 = 0.0;
			double delta3 = 0.0;
			double delta4 = 0.0;
			double dist = 0.0;
			
			
			for (int ridx = 0; ridx < p*k; ridx++) {
				for(int cidx = 0; cidx < p*k; cidx++) {
					delta1 = clusters[ridx][0] - clusters[cidx][0];
					delta2 = clusters[ridx][1] - clusters[cidx][1];
					delta3 = clusters[ridx][2] - clusters[cidx][2];
					delta4 = clusters[ridx][3] - clusters[cidx][3];
					
					dist = delta1*delta1 + delta2*delta2 + delta3*delta3 + delta4*delta4;
					distances[ridx][cidx] = Math.sqrt(dist);
				}
			}
			
			for (int ridx = 0; ridx < p*k; ridx++) {
				for(int cidx = 0; cidx < p*k; cidx++) {
					System.out.print(Double.toString(distances[ridx][cidx]) + "\t");
				}
				
				System.out.print("\n");
			}
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		
		fileReader.close();
		
		/*try {
			//Provide the attribute names
			FastVector attrbName = new FastVector();

			//Hard-coding attribute names for now
			Attribute attrb1 = new Attribute("preg");
			Attribute attrb2 = new Attribute("plas");
			Attribute attrb3 = new Attribute("pres");
			Attribute attrb4 = new Attribute("skin");
			Attribute attrb5 = new Attribute("insu");
			Attribute attrb6 = new Attribute("mass");
			Attribute attrb7 = new Attribute("pedi");
			Attribute attrb8 = new Attribute("age");
			Attribute clabel = new Attribute("class");
			
			attrbName.addElement(attrb1);
			attrbName.addElement(attrb2);
			attrbName.addElement(attrb3);
			attrbName.addElement(attrb4);
			attrbName.addElement(attrb5);
			attrbName.addElement(attrb6);
			attrbName.addElement(attrb7);
			attrbName.addElement(attrb8);
			attrbName.addElement(clabel);
			
			String instName = "diabetes";
			Instances actual= new Instances(instName, attrbName,1);
			actual.setClass(clabel);
			
			File file = new File("/Users/suchismit/eclipse/workspace/diabetes.csv");
			FileReader fileReader = new FileReader(file);
			BufferedReader bufferedReader = new BufferedReader(fileReader);
			
			String line;
			while ((line = bufferedReader.readLine()) != null) {
				String[] tokens = line.split(",");
				double[] values = new double[tokens.length];
				
				for (int idx = 0; idx < tokens.length; idx++) {
					values[idx] = Double.parseDouble(tokens[idx]);
				}

				Instance ist = new Instance(1, values);
				actual.add(ist);
			}
			
			fileReader.close();
			
			HierarchicalClusterer HC = new HierarchicalClusterer();
			HC.setNumClusters(2);
			EuclideanDistance dist = new EuclideanDistance();

			HC.setDistanceFunction(dist);
			
			HC.buildClusterer(actual);
			
			for (int idx = 0; idx < actual.numInstances(); idx++) {
                System.out.printf("(%.0f,%.0f): %s%n", actual.instance(idx).value(0), actual.instance(idx).value(1), HC.clusterInstance(actual.instance(idx)));
			}
			
			//System.out.println(HC.graph());
			
			//ClusteringResult result = new ClusteringResult(null, HC.toString(), HC.getNumClusters(), null);
			//System.out.println(result.getHiearchicalCluster());
			
		} catch (Exception e) {
			e.printStackTrace();
		}*/
	}
}
