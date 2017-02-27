package edu.sjtu.benchmark.community.procedures;
import org.apache.log4j.Logger;
import org.voltdb.*;
 
public class GetData extends VoltProcedure {
 
	private static final Logger LOG = Logger.getLogger(GetData.class);
    public final SQLStmt GetItem = new SQLStmt("SELECT * FROM ITEMS WHERE I_ID = ? ");
                                                                         
    public VoltTable[] run(long[] i_id) {
    	assert i_id.length > 0;
    	String x = "Get Data:";
    	
    	for (long i : i_id){
    		x += i;
    		voltQueueSQL(GetItem, i);
    	}
    	LOG.info(x);
        
        return (voltExecuteSQL());
    }
}
