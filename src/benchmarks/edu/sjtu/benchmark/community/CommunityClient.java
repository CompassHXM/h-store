package edu.sjtu.benchmark.community;
 
import java.io.IOException;
import java.util.Random;

import org.apache.log4j.Logger;
import org.voltdb.client.*;
import edu.brown.api.BenchmarkComponent;
 
public class CommunityClient extends BenchmarkComponent {

    private static final Logger LOG = Logger.getLogger(CommunityLoader.class);
    private static final int counter[] = new int[100];
    
    public static void main(String args[]) {
        BenchmarkComponent.main(CommunityClient.class, args, false);
    }
    
 
    public CommunityClient(String[] args) {
        super(args);
        for (String key : m_extraParams.keySet()) {
            // TODO: Retrieve extra configuration parameters
            if (key == "client_num"){
                String value = m_extraParams.get(key);
		System.out.println("value = "+ value);
                if (LOG.isDebugEnabled())
                    LOG.debug("key = " + key + ", value = " + value);
            }
        } // FOR
    }
 
    @Override
    public void runLoop() {
        try {
            Client client = this.getClientHandle();
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
        int param = rand.nextInt(CommunityProjectBuilder.PROCEDURES.length);;
//      System.out.println("Read " + param);
        params = new Object[]{ param };
        String procName = CommunityProjectBuilder.PROCEDURES[param].getSimpleName();
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
        String procNames[] = new String[CommunityProjectBuilder.PROCEDURES.length];
        for (int i = 0; i < procNames.length; i++) {
            procNames[i] = CommunityProjectBuilder.PROCEDURES[i].getSimpleName();
        }
        return (procNames);
    }
}
