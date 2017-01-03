package edu.sjtu.benchmark.linkbench.procedures;
import org.voltdb.*;
 
public class GetNode extends VoltProcedure {
 
    public final SQLStmt GetNode = new SQLStmt("SELECT * FROM nodetable WHERE id = ? ");
 
    public VoltTable[] run(long node_id) {
        voltQueueSQL(GetNode, node_id);
        return (voltExecuteSQL());
    }
}
