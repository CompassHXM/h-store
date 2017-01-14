package edu.sjtu.benchmark.linkbench.procedures;

import org.voltdb.*;
import edu.sjtu.benchmark.linkbench.LinkbenchConstants;

public class GetLink extends VoltProcedure {
	
	 public final SQLStmt getLink = new SQLStmt(
	            " select id1, id2, link_type," +
	            " visibility, data, time, " +
	            " version from linktable "+
	            " where id1 = ? and link_type = ? " +
	            " and id2 in (?)"
	    );
	 public VoltTable[] run() {
	    	voltQueueSQL(getLink);
	    	return voltExecuteSQL(true);
	    }
	
}