package edu.sjtu.benchmark.linkbench.procedures;

import org.voltdb.*;
import edu.sjtu.benchmark.linkbench.LinkbenchConstants;

public class GetNodesFromLink extends VoltProcedure {
	 public final SQLStmt getLinks = new SQLStmt(
		        "SELECT id2 FROM " + LinkbenchConstants.TABLENAME_LINK +
		        " WHERE id1 = ?"
		    );
	 public final SQLStmt getnodes = new SQLStmt(
			 "SELECT * FROM " + LinkbenchConstants.TABLENAME_NODE +
			 " WHERE id = ?"
			 );
	 public VoltTable[] run(int nid) {
		 voltQueueSQL(getLinks, nid);
	     final VoltTable result[] = voltExecuteSQL();
	     assert result.length == 1;
	     
	     
	     
	     
	     
	     
	     
	     
	     
	     
	     return null;
	 }
}