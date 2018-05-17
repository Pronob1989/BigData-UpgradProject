package upgrad.pgpbde.ds.linear.hashtable;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Hashtable;
import java.util.Map;
import java.util.Scanner;

import javax.management.openmbean.KeyAlreadyExistsException;

/**
 *  The <tt>ElectionCount</tt> class represents a db of voted information. 
 *  It supports insertion and find operation in arbitrary order.
 *  <p>
 *  This implementation uses java built in Hashtable to store the voting information.
 *  Have <em>ADD</em>, <em>FIND</em>, and <em>COUNT</em> operations
 */
public class ElectionCount<VoterId extends Comparable<VoterId>, CandidateId> {
    private static final int INVALID_ENTRY = -1;
	private Hashtable<VoterId, CandidateId> evmOutputDb;
	private Hashtable<CandidateId, Integer> candidateVoteCountDb;
	
    /**
     * ctor Instantiate the HastTable instance for db.
     */
	public ElectionCount() {
		evmOutputDb = new Hashtable<VoterId, CandidateId>();
		candidateVoteCountDb = new Hashtable<CandidateId, Integer>();
	}
	
    /**
     * Add entry into the db using passed voter id as Key
     * Takes O(n) worst case
     *  O(1) amortized run time because the time required to insert an element is O(1) on the average
     * @param vId Id of the voter
     * @param cId Id of the candidate
     * @throws KeyAlreadyExistsException if voter id key is present in db already
     */
	public void ADD(VoterId vId, CandidateId cId) {
	    if (evmOutputDb.containsKey(vId)) {
	        throw new KeyAlreadyExistsException("Voter Id: " + vId + " Already Voted");
	    }
		evmOutputDb.put(vId, cId);
		int count = 0;
		if (candidateVoteCountDb.containsKey(cId)) {
			count = candidateVoteCountDb.get(cId);
		}
		count++;
		candidateVoteCountDb.put(cId, count);
		System.out.println("Adding vid: " + vId + " cid: " + cId );
	}
	
    /**
     * Find the voted candidate information for the given voter id from db
     * Takes O(n/m) in average case, if n is the number of items and m is the size
     * Takes O(n) worst case and O(1) best case time
     * @param vId Id of the voter
     * @return Id of candidate for voter if present; <tt>null</tt> otherwise
     */
	public CandidateId FIND(VoterId vId) {
	    if (evmOutputDb.containsKey(vId)) {
	        return evmOutputDb.get(vId);
	    }
	    return null;
	}
	
    /**
     * Get the number of votes received by the specified candidate
     * Takes O(n/m) in average, if n is the number of items and m is the size
     * Takes O(n) worst case and O(1) best case time
     * @param cId Id of the candidate
     * @return number of votes received by the candidate if present; <tt>0</tt> otherwise
     */
	public int COUNT(CandidateId cId) {
	    if (candidateVoteCountDb.containsKey(cId)) {
	        return candidateVoteCountDb.get(cId);
	    }
	    return 0;
	}
	
    /**
     * Display the entries in the DB
     */
    public void SHOW() {
        for(Map.Entry entry: evmOutputDb.entrySet()) {
            System.out.println("Voter Id: " + entry.getKey() + " Candidate Id " +  entry.getValue());
        }
    }
	
    /**
     *  Unit tests the <tt>ElectionCount</tt> data type.
     *  @param args takes the optional data file name as parameter
     *  <p>
     *  Compilation:  javac ElectionCount.java
     *  <p> 
     *  Option 1) 
     *  <p>
     *     Execution:  java ElectionCount data.txt
     *  <p>
     *     - Uses data from file to populate the db and also ask user input to perform ADD/FIND and
     *     COUNT operation. 
     *  <p>
     *  % more data.txt 
     *  151020  130
     *  151021  135
     *  151022  132
     *  151023  135
     *  <p>
     * Option 2) 
     *  <p>
     *     Execution:  java ElectionCount
     *  <p>
     *     - If there is no data file passed as input parameter then ask user for
     *     operations (ADD/FIND/COUNT) to perform.
     */
	public static void main(String[] args) {
        // TODO Auto-generated method stub
        ElectionCount<Integer, Integer> ec = new ElectionCount<Integer, Integer>();
        Scanner input = new Scanner(System.in);
        // check for input file parameter in argument list
        if (args.length == 1) {
            try {
                BufferedReader in = null;
                in = new BufferedReader(new FileReader(args[0]));
                String line = null; 
                while ((line = in.readLine()) != null) {
                    String[] id = line.split("\t");
                    if (id.length != 2) {
                        throw (new IllegalArgumentException());
                    }
                    ec.ADD(Integer.parseInt(id[0]), Integer.parseInt(id[1]));
                }
                System.out.println("Used input file "+ args[0] + " to update DB");

            } catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        } 

        while(true) {
            System.out.println("Operation Key: 1 - ADD, 2 - FIND, 3 - COUNT, 4 - SHOW, 5 - EXIT");
            System.out.println("Enter any Key: ");
            int key = input.nextInt();
            int vid = INVALID_ENTRY;
            int cid = INVALID_ENTRY;
            switch(key) {
                case 1:
                    System.out.println("ADD Operation ");
                    System.out.println("Enter voter id: ");
                    vid = input.nextInt();
                    System.out.println("Enter candidate id: ");
                    cid = input.nextInt();
                    ec.ADD(vid, cid);
                    break;
                case 2:
                    System.out.println("FIND Operation");
                    System.out.println("Enter voter id: ");
                    vid = input.nextInt();
                    System.out.println("Voter: " + vid + " Voted for Candidate: "+ ec.FIND(vid));
                    break;
                case 3:
                    System.out.println("COUNT Operation");
                    System.out.println("Enter candidate id: ");
                    cid = input.nextInt();
                    System.out.println("Candidate: " + cid + " total vote count: "+ ec.COUNT(cid));
                    break;
                case 4:
                    System.out.println("SHOW Operation");
                    ec.SHOW();
                    break;
                case 5:
                    System.out.println("EXIT Operation");
                    input.close();
                    return;

            }
        }
    }

}

