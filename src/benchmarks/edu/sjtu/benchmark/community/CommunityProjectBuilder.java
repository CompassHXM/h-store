package edu.sjtu.benchmark.community;
 
import edu.brown.benchmark.AbstractProjectBuilder;
import edu.brown.api.BenchmarkComponent;
import edu.sjtu.benchmark.community.procedures.*;
 
public class CommunityProjectBuilder extends AbstractProjectBuilder {
 
    // REQUIRED: Retrieved via reflection by BenchmarkController
    public static final Class<? extends BenchmarkComponent> m_clientClass = CommunityClient.class;
 
    // REQUIRED: Retrieved via reflection by BenchmarkController
    public static final Class<? extends BenchmarkComponent> m_loaderClass = CommunityLoader.class;
 
    public static final Class<?> PROCEDURES[] = new Class<?>[] {
        GetData.class,
    };
    public static final String PARTITIONING[][] = new String[][] {
        // { "TABLE NAME", "PARTITIONING COLUMN NAME" }
        {"TABLEA", "I_ID"},
    };
 
    public CommunityProjectBuilder() {
        super("community", CommunityProjectBuilder.class, PROCEDURES, PARTITIONING);
 
        // Create a single-statement stored procedure named 'DeleteData'
        addStmtProcedure("DeleteData", "DELETE FROM TABLEA WHERE I_ID < ?");
    }
}