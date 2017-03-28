package edu.sjtu.benchmark.community.procedures;
import org.apache.log4j.Logger;
import org.voltdb.*;
@ProcInfo(
	singlePartition = false
) 
public class GetData extends VoltProcedure {
 
	public final SQLStmt GetItem = new SQLStmt("SELECT * FROM ITEMS WHERE I_ID = ? ");
                                                                         

	public VoltTable[] run(long i_id) {
			voltQueueSQL(GetItem, i_id);
			return (voltExecuteSQL(true));
    	}

}
