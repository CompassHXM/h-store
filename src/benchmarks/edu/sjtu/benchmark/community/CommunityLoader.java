package edu.sjtu.benchmark.community;
 
import java.util.Random;

import org.apache.log4j.Logger;
import org.voltdb.CatalogContext;
import org.voltdb.catalog.*;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;

import edu.brown.api.BenchmarkComponent;
import edu.brown.api.Loader;
import edu.brown.benchmark.wikipedia.util.TextGenerator;
import edu.brown.catalog.CatalogUtil;
 
public class CommunityLoader extends Loader {
 
    private final Random rng = new Random();
    private static final Logger LOG = Logger.getLogger(CommunityLoader.class);
    
    public static void main(String args[]) throws Exception {
        BenchmarkComponent.main(CommunityLoader.class, args, true);
       
    }

    public CommunityLoader(String[] args) {
        super(args);
        for (String key : m_extraParams.keySet()) {
            if (key == "data_file") {
                String value = m_extraParams.get(key);
                if (LOG.isDebugEnabled())
                    LOG.debug("key = " + key + ", value = " + value);
            }
            // TODO: Retrieve extra configuration parameters
        } // FOR
    }
 
    @Override
    public void load() {
        // The catalog contains all the information about the database (e.g., tables, columns, indexes)
        // It is loaded from the benchmark's project JAR file
        final CatalogContext catalogContext = this.getCatalogContext();
        Table catalog_tbl = catalogContext.database.getTables().getIgnoreCase("items");
        assert(catalog_tbl != null);
        VoltTable vt = CatalogUtil.getVoltTable(catalog_tbl);
        int num_cols = catalog_tbl.getColumns().size();
        
        for (int i=0;i<10;i++)
        {
            Object row[] = new Object[num_cols];
            String name = TextGenerator.randomStr(rng,10);
            row[0] = i;
            row[1] = name;
            row[2] = 10;
            
            vt.addRow(row);
            this.loadVoltTable(catalog_tbl.getName(), vt);
            vt.clearRowData();
        }
        LOG.info("Loading 10 finished.");
    }
}
