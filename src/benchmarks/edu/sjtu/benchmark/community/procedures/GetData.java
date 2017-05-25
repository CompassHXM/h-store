package edu.sjtu.benchmark.community.procedures;
import org.apache.log4j.Logger;
import org.voltdb.*;
@ProcInfo(
	singlePartition = false
) 
public class GetData extends VoltProcedure {
 
	public final SQLStmt GetItem = new SQLStmt("SELECT * FROM items WHERE id = ? ");
                                                                         

	public VoltTable[] run(long id1, long id2) {
			voltQueueSQL(GetItem, id1);
			voltQueueSQL(GetItem, id2);
			return (voltExecuteSQL(true));
    	}

}
