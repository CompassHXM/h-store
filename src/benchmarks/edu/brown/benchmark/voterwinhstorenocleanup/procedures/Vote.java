/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

//
// Accepts a vote, enforcing business logic: make sure the vote is for a valid
// contestant and that the voterwinhstorenocleanup (phone number of the caller) is not above the
// number of allowed votes.
//

package edu.brown.benchmark.voterwinhstorenocleanup.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.StmtInfo;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.types.TimestampType;

import edu.brown.benchmark.voterwinhstorenocleanup.VoterWinHStoreNoCleanupConstants;

@ProcInfo (
    partitionInfo = "votes.phone_number:1",
    singlePartition = true
)
public class Vote extends VoltProcedure {
	
    // Checks if the vote is for a valid contestant
    public final SQLStmt checkContestantStmt = new SQLStmt(
	   "SELECT contestant_number FROM contestants WHERE contestant_number = ?;"
    );
	
    // Checks if the voterwinhstorenocleanup has exceeded their allowed number of votes
    public final SQLStmt checkVoterWinHStoreNoCleanupStmt = new SQLStmt(
		"SELECT num_votes FROM v_votes_by_phone_number WHERE phone_number = ?;"
    );
	
    // Checks an area code to retrieve the corresponding state
    public final SQLStmt checkStateStmt = new SQLStmt(
		"SELECT state FROM area_code_state WHERE area_code = ?;"
    );
    
    public final SQLStmt getMaxVoteIDStmt = new SQLStmt(
		"SELECT value FROM state WHERE row_id = 2;"
    );
    
    public final SQLStmt getStagingCountStmt = new SQLStmt(
		"SELECT value FROM state WHERE row_id = 1;"
    );
    
    public final SQLStmt getVoteCountStmt = new SQLStmt(
		"SELECT count(*) FROM votes;"
    );
	
    // Records a vote
    public final SQLStmt insertVoteStmt = new SQLStmt(
		"INSERT INTO votes (vote_id, phone_number, state, contestant_number, created) VALUES (?, ?, ?, ?, ?);"
    );
    
    public final SQLStmt updateMaxVoteIDStmt = new SQLStmt(
		"UPDATE state SET value = ? WHERE row_id = 2;"
    );
    
    @StmtInfo(
            upsertable=true
        )
    public final SQLStmt updateStagingCountStmt = new SQLStmt(
    	"INSERT INTO state (row_id, value) SELECT row_id, value + 1 FROM state WHERE row_id = 1;"
    );
    
    public final SQLStmt resetStagingCountStmt = new SQLStmt(
    	"UPDATE state SET value = 0 WHERE row_id = 1;"
    );
    
    public final SQLStmt clearWindowStmt = new SQLStmt(
    	"DELETE FROM W_ROWS;"	
    );
    
    public final SQLStmt getLowestVoteIDStmt = new SQLStmt(
    	"SELECT vote_id FROM votes ORDER BY vote_id DESC LIMIT ?;"
    );
    
    public final SQLStmt getWindowStmt = new SQLStmt(
    	"SELECT contestant_number, count(*) FROM votes WHERE vote_id >= ? GROUP BY CONTESTANT_NUMBER ORDER BY CONTESTANT_NUMBER;"
    );
    
    public final SQLStmt getLeaderboardStmt = new SQLStmt(
    	"SELECT * FROM v_leaderboard;"
    );
	
    public long run(long voteId, long phoneNumber, int contestantNumber, long maxVotesPerPhoneNumber) {
		
        // Queue up validation statements
        voltQueueSQL(checkContestantStmt, contestantNumber);
        voltQueueSQL(checkVoterWinHStoreNoCleanupStmt, phoneNumber);
        voltQueueSQL(checkStateStmt, (short)(phoneNumber / 10000000l));
        voltQueueSQL(getVoteCountStmt);
        VoltTable validation[] = voltExecuteSQL();
		
        if (validation[0].getRowCount() == 0) {
            return VoterWinHStoreNoCleanupConstants.ERR_INVALID_CONTESTANT;
        }
		
        if ((validation[1].getRowCount() == 1) &&
			(validation[1].asScalarLong() >= maxVotesPerPhoneNumber)) {
            return VoterWinHStoreNoCleanupConstants.ERR_VOTER_OVER_VOTE_LIMIT;
        }
		
        // Some sample client libraries use the legacy random phone generation that mostly
        // created invalid phone numbers. Until refactoring, re-assign all such votes to
        // the "XX" fake state (those votes will not appear on the Live Statistics dashboard,
        // but are tracked as legitimate instead of invalid, as old clients would mostly get
        // it wrong and see all their transactions rejected).
        final String state = (validation[2].getRowCount() > 0) ? validation[2].fetchRow(0).getString(0) : "XX";
        long votecount = (validation[3].getRowCount() > 0) ? validation[3].fetchRow(0).getLong(0) : 0;
        
        // Post the vote
        TimestampType timestamp = new TimestampType();
        voltQueueSQL(insertVoteStmt, voteId, phoneNumber, state, contestantNumber, timestamp);
        voltQueueSQL(updateStagingCountStmt);
        voltQueueSQL(getStagingCountStmt);
        validation = voltExecuteSQL();
        
        long stagingCount = validation[2].fetchRow(0).getLong(0);
                
        
        if(votecount >= (VoterWinHStoreNoCleanupConstants.WINDOW_SIZE + VoterWinHStoreNoCleanupConstants.SLIDE_SIZE) && stagingCount >= VoterWinHStoreNoCleanupConstants.SLIDE_SIZE)
        {
        	voltQueueSQL(resetStagingCountStmt);
        	voltQueueSQL(getLowestVoteIDStmt, VoterWinHStoreNoCleanupConstants.WINDOW_SIZE);
        	validation = voltExecuteSQL();
        	long lowestVoteID = validation[1].fetchRow((int)(VoterWinHStoreNoCleanupConstants.WINDOW_SIZE-1)).getLong(0);
        	voltQueueSQL(getWindowStmt, lowestVoteID);
        	voltExecuteSQL(true);
        }
        
        // Set the return value to 0: successful vote
        return VoterWinHStoreNoCleanupConstants.VOTE_SUCCESSFUL;
    }
}