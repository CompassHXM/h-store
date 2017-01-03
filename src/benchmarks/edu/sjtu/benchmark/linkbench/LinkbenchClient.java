package edu.sjtu.benchmark.linkbench;
 
import java.io.IOException;
import java.util.Random;

import org.apache.log4j.Logger;
import org.voltdb.client.*;
import edu.brown.api.BenchmarkComponent;
 
public class LinkbenchClient extends BenchmarkComponent {

    private static final Logger LOG = Logger.getLogger(LinkbenchClient.class);
    private static final int counter[] = new int[100];
    
    public static void main(String args[]) {
        BenchmarkComponent.main(LinkbenchClient.class, args, false);
    }
    
 
    public LinkbenchClient(String[] args) {
        super(args);
        for (String key : m_extraParams.keySet()) {
            // TODO: Retrieve extra configuration parameters
            if (key == "client_num"){
                String value = m_extraParams.get(key);
                if (LOG.isDebugEnabled())
                    LOG.debug("key = " + key + ", value = " + value);
            }
        } // FOR
    }
 
    @Override
    public void runLoop() {
        try {
            Client client = this.getClientHandle();
            LOG.info("OUR Client is Running now!");
            while (true) {
            	runOnce();
                client.backpressureBarrier();
            } // WHILE
        } catch (NoConnectionsException e) {
            // Client has no clean mechanism for terminating with the DB.
            return;
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (IOException e) {
            // At shutdown an IOException is thrown for every connection to
            // the DB that is lost Ignore the exception here in order to not
            // get spammed, but will miss lost connections at runtime
        }
    }
 
    @Override
    public boolean runOnce() throws IOException {
    	Object params[];
    	Random rand = new Random();
        int param = rand.nextInt(LinkbenchProjectBuilder.PROCEDURES.length);;
//      System.out.println("Read " + param);
        params = new Object[]{ param };
        String procName = LinkbenchProjectBuilder.PROCEDURES[param].getSimpleName();
        assert(params != null);
        
        Callback callback = new Callback(param);
        return this.getClientHandle().callProcedure(callback, procName, params);
    } 
    private class Callback implements ProcedureCallback {
        private final int idx;
 
        public Callback(int idx) {
            this.idx = idx;
        }
        @Override
        public void clientCallback(ClientResponse clientResponse) {
            // Increment the BenchmarkComponent's internal counter on the
            // number of transactions that have been completed
            incrementTransactionCounter(clientResponse,this.idx);
        }
    } // END CLASS
 
    @Override
    public String[] getTransactionDisplayNames() {
        // Return an array of transaction names
        String procNames[] = new String[LinkbenchProjectBuilder.PROCEDURES.length];
        for (int i = 0; i < procNames.length; i++) {
            procNames[i] = LinkbenchProjectBuilder.PROCEDURES[i].getSimpleName();
        }
        return (procNames);
    }
}
