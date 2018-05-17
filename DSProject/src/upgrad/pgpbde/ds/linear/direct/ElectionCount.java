
package upgrad.pgpbde.ds.linear.direct;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Scanner;
import javax.management.openmbean.KeyAlreadyExistsException;

/**
 *  The <tt>ElectionCount</tt> class represents a db of voted information. 
 *  It supports insertion and find operation in arbitrary order.
 *  <p>
 *  This implementation uses a fixed array to store the voting information
 *  using direct addressing method.
 *  The <em>ADD</em>, <em>FIND</em>, and <em>COUNT</em> operations
 *  take constant time O(1).
 */
public class ElectionCount {
    private static final int INIT_MIN_VOTER_ID = 100000;
    private static final int INIT_MAX_VOTER_ID = 999999;
    private static final int INIT_MIN_CANDIDATE_ID = 100;
    private static final int INIT_MAX_CANDIDATE_ID = 999;
    private static final int INVALID_ENTRY = -1;
    
    private int minVoterId;
	private int[] evmOutputDb;
	private int minCandidateId;
	private int[] candidateVoteCountDb;
	
	private int getVoterIdx(int voterId) {
	    return (voterId-minVoterId);
	}
	
	private String getVoterIdRange() {
	    return ("("+ minVoterId + "," + (minVoterId+evmOutputDb.length-1) + ")");
	}
	
	private int getCandidateIdx(int candidateId) {
	    return (candidateId-minCandidateId);
	}
	
	private String getCandidateIdRange() {
	    return ("("+ minCandidateId + "," + (minCandidateId+candidateVoteCountDb.length-1) + ")");
	}
	
    /**
     * ctor allocate the memory for db as per default values.
     */
    public ElectionCount() {
        this(INIT_MIN_VOTER_ID, INIT_MAX_VOTER_ID, INIT_MIN_CANDIDATE_ID, INIT_MAX_CANDIDATE_ID);
    }
    
	/**
	 * ctor allocate the memory for db as per the params.
	 * @param minVoterId   Minimum value of Voter Id
	 * @param maxVoterId   Maximum value of Voter Id
	 * @param minCadidateId    Minimum value of Candidate Id
	 * @param maxcandidatId    Maximum value of Candidate Id
	 */
	public ElectionCount(int minVoterId, int maxVoterId, int minCadidateId, int maxcandidatId) {
	    this.minVoterId = minVoterId;
	    this.minCandidateId = minCadidateId;
	    
		evmOutputDb = new int[(maxVoterId+1)-minVoterId];
		for(int idx = 0; idx < evmOutputDb.length; idx++) {
			evmOutputDb[idx] = INVALID_ENTRY;
		}
		candidateVoteCountDb = new int[(maxcandidatId+1) - minCadidateId];
		for(int idx = 0; idx < candidateVoteCountDb.length; idx++) {
			candidateVoteCountDb[idx] = 0;
		}
	}
	
    /**
     * Add entry into the db using passed id as index
     * Takes O(1) constant time to insert the element into the db using the index
	 * @param voterId Id of the voter
	 * @param candidateId Id of the candidate
	 * @throws IllegalArgumentException if input is out of range
	 * @throws KeyAlreadyExistsException if voter id entry is present in db already
	 */
	public void ADD(int voterId, int candidateId) {
	    int vidx = getVoterIdx(voterId);
	    if ((vidx < 0) || (vidx >= evmOutputDb.length)) {
	        throw new IllegalArgumentException("Voter Id: " + voterId + " is Out of Range: "+ getVoterIdRange());
	    }
	    
	    int cidx = getCandidateIdx(candidateId);
	    if ((cidx < 0) || (cidx >= candidateVoteCountDb.length)) {
	        throw new IllegalArgumentException("Candidate Id: " +  candidateId + " is Out of Range: " + getCandidateIdRange());
	    }
	    
	    if (evmOutputDb[vidx] != INVALID_ENTRY) {
	        throw new KeyAlreadyExistsException("Voter Id: " + voterId + " Already Voted");
	    }
	    evmOutputDb[vidx] = candidateId;
		candidateVoteCountDb[cidx]++;
	}
	
    /**
     * Find the voted candidate information for the given voter from db
     * Takes O(1) worst time to find the element in db using the index
	 * @param voterId Id of the voter
     * @return Id of candidate for voter if present; <tt>-1</tt> otherwise
     */
	public int FIND(int voterId) {
	    int vidx = getVoterIdx(voterId);
	    if ((vidx < 0) || (vidx >= evmOutputDb.length)) {
	        return INVALID_ENTRY;
	    }
	    if (evmOutputDb[vidx] == INVALID_ENTRY) {
	        return INVALID_ENTRY;
	    }
		return evmOutputDb[vidx];
	}
	
    /**
     * Get the number of votes received by the specified candidate
     * Takes O(1) worst time to find the element in db using the index
	 * @param candidateId Id of the candidate
     * @return number of votes received by the candidate if present; <tt>0</tt> otherwise
     */
	public int COUNT(int candidateId) {
	    int cidx = getCandidateIdx(candidateId);
        if ((cidx < 0) || (cidx >= candidateVoteCountDb.length)) {
            return 0;
        }
		return candidateVoteCountDb[cidx];
	}
	
    /**
     * Display the entries in the DB
     */
	public void SHOW() {
	    for(int idx = 0; idx < evmOutputDb.length; idx++) {
	        if (evmOutputDb[idx] != INVALID_ENTRY) {
	            System.out.println("Voter Id: " + (idx + minVoterId) + " Candidate Id " +  evmOutputDb[idx]);
	        }
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
	 *     - uses default range values for voter id (100000,999999) and candidate id (100,999)
	 *     - If the data file uses different range values for voter/candidate ids then call
	 *     the respective constructor {@link #ElectionCount} to pass the range values accordingly.
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
     *     - If there is no data file passed as input parameter then ask user for explicit range values and
     *     operations (ADD/FIND/COUNT) to perform.
	 */
	public static void main(String[] args) {
	    // TODO Auto-generated method stub
	    ElectionCount ec = null;
	    Scanner input = new Scanner(System.in);
	    // check for input file parameter in argument list
	    if (args.length == 1) {
	        try {
	            BufferedReader in = null;
	            in = new BufferedReader(new FileReader(args[0]));
	            String line = null;
	            ec = new ElectionCount(); 

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
	    } else {
	        System.out.println("Enter Voter Min Id value: ");
	        int voterIdMin = input.nextInt();
	        System.out.println("Enter Voter Max Id value: ");
	        int voterIdMax = input.nextInt();
	        System.out.println("Enter Candidate Min Id value: ");
	        int candidateIdMin = input.nextInt();
	        System.out.println("Enter Candidate Max Id value: ");
	        int candidateIdMax = input.nextInt();
	        ec = new ElectionCount(voterIdMin, voterIdMax, candidateIdMin, candidateIdMax);
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
