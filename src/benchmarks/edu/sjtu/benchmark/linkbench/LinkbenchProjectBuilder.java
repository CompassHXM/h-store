package edu.sjtu.benchmark.linkbench;
 
import edu.brown.benchmark.AbstractProjectBuilder;

import org.voltdb.VoltProcedure;

import edu.brown.api.BenchmarkComponent;
import edu.sjtu.benchmark.linkbench.procedures.*;
 
public class LinkbenchProjectBuilder extends AbstractProjectBuilder {
 
    // REQUIRED: Retrieved via reflection by BenchmarkController
    public static final Class<? extends BenchmarkComponent> m_clientClass = LinkbenchClient.class;
 
    // REQUIRED: Retrieved via reflection by BenchmarkController
    public static final Class<? extends BenchmarkComponent> m_loaderClass = LinkbenchLoader.class;
 
    @SuppressWarnings("unchecked")
    public static final Class<? extends VoltProcedure> PROCEDURES[] = (Class<? extends VoltProcedure>[]) new Class<?>[] {
        GetData.class
    };
    
    public static final String PARTITIONING[][] = new String[][] {
        // { "TABLE NAME", "PARTITIONING COLUMN NAME" }
        {"ITEMS", "I_ID"}
    };
 
    public LinkbenchProjectBuilder() {
        super("linkbench", LinkbenchProjectBuilder.class, PROCEDURES, PARTITIONING);
        // Create a single-statement stored procedure named 'DeleteData'
//        addStmtProcedure("DeleteData", "DELETE FROM ITEMS WHERE I_ID < ?");
    }
}
