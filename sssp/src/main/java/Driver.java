
public class Driver {

    public static void main(String[] args) throws Exception {
        Preprocess prep = new Preprocess();
        ShortestPath sf = new ShortestPath();

        // parse args
	String inputPath = args[0];
	String subInputPath = args[1];
	String outputPath = args[2];
	
        /*String matrixInputPath = args[0];
        String vectorInputPath = args[1];
        String subSumOutputPath = args[2];
        String sumOutputPath = args[3];*/

        // run the first job
        String[] prepArgs = {inputPath, subInputPath};
        prep.main(prepArgs);

        // run the second job
        String[] sfArgs = {subInputPath, outputPath};
        sf.main(sfArgs);
    }
}
