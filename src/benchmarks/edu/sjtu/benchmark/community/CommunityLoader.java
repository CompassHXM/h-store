package edu.sjtu.benchmark.community
 
import org.voltdb.catalog.*;
import org.voltdb.client.Client;
import edu.brown.api.Loader;
import edu.brown.catalog.CatalogUtil;
 
public class CommunityLoader extends Loader {
 
    public static void main(String args[]) throws Exception {
        BenchmarkComponent.main(ABCLoader.class, args, true);
    }
 
    public CommunityLoader(String[] args) {
        super(args);
        for (String key : m_extraParams.keySet()) {
            if (key == "data_file") {
                //String value = ;
                //print some log
            }
            // TODO: Retrieve extra configuration parameters
        } // FOR
    }
 
    @Override
    public void load() {
        // The catalog contains all the information about the database (e.g., tables, columns, indexes)
        // It is loaded from the benchmark's project JAR file
        Catalog catalog = this.getCatalog();
 
        // Iterate over all of the Table handles in the catalog and generate
        // tuples to upload into the database
        for (Table catalog_tbl : CatalogUtil.getDatabase(catalog).getTables()) {
            // TODO: Create an empty VoltTable handle and then populate it in batches to 
            //       be sent to the DBMS
            VoltTable table = CatalogUtil.getVoltTable(catalog_tbl);
 
            // Invoke the BenchmarkComponent's data loading method
            // This will upload the contents of the VoltTable into the DBMS cluster
            this.loadVoltTable(catalog_tbl.getName(), table);
        } // FOR
    }
}