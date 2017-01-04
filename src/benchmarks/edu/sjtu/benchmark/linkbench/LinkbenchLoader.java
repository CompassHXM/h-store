package edu.sjtu.benchmark.linkbench;
 
import java.io.IOException;
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
 
public class LinkbenchLoader extends Loader {
 
    private final Random rng = new Random();
    private static final Logger LOG = Logger.getLogger(LinkbenchLoader.class);
    
    public static void main(String args[]) throws Exception {
        BenchmarkComponent.main(LinkbenchLoader.class, args, true);
       
    }

    public LinkbenchLoader(String[] args) {
        super(args);
        for (String key : m_extraParams.keySet()) {
            if (key == "network_file") {
                String value = m_extraParams.get(key);
                if (LOG.isDebugEnabled())
                    LOG.debug("key = " + key + ", value = " + value);
            }
            // TODO: Retrieve extra configuration parameters
        } // FOR
    }
    protected void loadNodes(Database catalog_db) throws IOException {
    	Table catalog_tbl_nodes = catalog_db.getTables().getIgnoreCase(LinkbenchConstants.TABLENAME_NODE);
    	assert(catalog_tbl_nodes != null);
    	VoltTable vt_nodes = CatalogUtil.getVoltTable(catalog_tbl_nodes);
    	int num_cols_nodes = catalog_tbl_nodes.getColumns().size();
    	
    }
    @Override
    public void load() throws IOException {
    	final CatalogContext catalogContext = this.getCatalogContext();
    	loadNodes(catalogContext.database);
    }
}
