//import FriendList;
//import MutualFriends;

public class Driver {

    public static void main(String[] args) throws Exception {
        FriendList frndlist = new FriendList();
        MutualFriends mfrnd = new MutualFriends();

        // parse args
	String inputPath = args[0];
	String subInputPath = args[1];
	String outputPath = args[2];
	
        /*String matrixInputPath = args[0];
        String vectorInputPath = args[1];
        String subSumOutputPath = args[2];
        String sumOutputPath = args[3];*/

        // run the first job
        String[] frndlistArgs = {inputPath, subInputPath};
        frndlist.main(frndlistArgs);

        // run the second job
        String[] mfrndArgs = {subInputPath, outputPath};
        mfrnd.main(mfrndArgs);
    }
}
