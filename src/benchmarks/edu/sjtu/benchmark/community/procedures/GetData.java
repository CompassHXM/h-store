package edu.sjtu.benchmark.community.procedures;
import org.voltdb.*;
 
public class GetData extends VoltProcedure {
 
    public final SQLStmt GetItem = new SQLStmt("SELECT * FROM ITEMS WHERE I_ID = ? ");
                                                                         
    public VoltTable[] run(long[] i_id) {
    	assert i_id.length > 0;
    	for (long i : i_id){
    		voltQueueSQL(GetItem, i);
    	}
        
        return (voltExecuteSQL());
    }
}
