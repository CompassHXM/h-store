package edu.mit.hstore.dtxn;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.pool.ObjectPool;
import org.apache.commons.pool.impl.StackObjectPool;
import org.apache.log4j.Logger;
import org.voltdb.VoltTable;
import org.voltdb.messaging.FragmentTaskMessage;

import edu.brown.utils.CountingPoolableObjectFactory;
import edu.brown.utils.LoggerUtil;
import edu.brown.utils.Poolable;
import edu.brown.utils.LoggerUtil.LoggerBoolean;
import edu.mit.hstore.HStoreConf;

/**
 * 
 * @author pavlo
 */
public class DependencyInfo implements Poolable {
    protected static final Logger LOG = Logger.getLogger(DependencyInfo.class);
    private final static LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private final static LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    /**
     * Object Pool Factory
     */
    private static class Factory extends CountingPoolableObjectFactory<DependencyInfo> {
        public Factory(boolean enable_tracking) {
            super(enable_tracking);
        }
        @Override
        public DependencyInfo makeObjectImpl() throws Exception {
            return (new DependencyInfo());
        }   
    }
    
    /**
     * DependencyInfo Object Pool
     */
    public static ObjectPool INFO_POOL;
    
    /**
     * Initialize the global object pool
     * @param hstore_conf
     */
    public synchronized static void initializePool(HStoreConf hstore_conf) {
        if (INFO_POOL == null) {
            INFO_POOL = new StackObjectPool(new Factory(hstore_conf.pool_enable_tracking), hstore_conf.pool_dependencyinfos_idle);
        }
    }
    
    // ----------------------------------------------------------------------------
    // INVOCATION DATA MEMBERS
    // ----------------------------------------------------------------------------
    
    protected LocalTransactionState ts;
    protected int stmt_index = -1;
    protected int dependency_id = -1;
    
    /**
     * List of PartitionIds that we expect to get responses/results back
     */
    protected final List<Integer> partitions = new ArrayList<Integer>();
    
    /**
     * The list of PartitionIds that have sent results
     */
    protected final List<Integer> results = new ArrayList<Integer>();

    /**
     * The list of VoltTable results that have been sent back partitions
     * We store it as a list so that we don't have to convert it for ExecutionSite
     */
    protected final List<VoltTable> results_list = new ArrayList<VoltTable>();
    
    /**
     * List of PartitionIds that have sent responses
     */
    protected final List<Integer> responses = new ArrayList<Integer>();
    
    /**
     * We assume a 1-to-n mapping from DependencyInfos to blocked FragmentTaskMessages
     */
    protected final Set<FragmentTaskMessage> blocked_tasks = new HashSet<FragmentTaskMessage>();
    protected boolean blocked_tasks_released = false;
    protected boolean blocked_all_local = true;
    
    /**
     * Constructor
     * @param stmt_index
     * @param dependency_id
     */
    private DependencyInfo() {
        // Nothing...
    }
    
    public void init(LocalTransactionState ts, int stmt_index, int dependency_id) {
        this.ts = ts;
        this.stmt_index = stmt_index;
        this.dependency_id = dependency_id;
    }

    @Override
    public boolean isInitialized() {
        return (this.ts != null);
    }
    
    @Override
    public void finish() {
        this.ts = null;
        this.stmt_index = -1;
        this.dependency_id = -1;
        this.partitions.clear();
        this.results.clear();
        this.results_list.clear();
        this.responses.clear();
        this.blocked_tasks.clear();
        this.blocked_tasks_released = false;
        this.blocked_all_local = true;
    }
    
    
    public int getStatementIndex() {
        return (this.stmt_index);
    }
    public int getDependencyId() {
        return (this.dependency_id);
    }
    public List<Integer> getPartitions() {
        return (this.partitions);
    }
    
    /**
     * Add a FragmentTaskMessage this blocked until all of the partitions return results/responses
     * for this DependencyInfo
     * @param ftask
     */
    public void addBlockedFragmentTaskMessage(FragmentTaskMessage ftask) {
        this.blocked_tasks.add(ftask);
        this.blocked_all_local = this.blocked_all_local && (ftask.getDestinationPartitionId() == this.ts.source_partition);
    }
    
    /**
     * Return the set of FragmentTaskMessages that are blocked until all of the partitions
     * return results/responses for this DependencyInfo 
     * @return
     */
    protected Set<FragmentTaskMessage> getBlockedFragmentTaskMessages() {
        return (this.blocked_tasks);
    }
    
    /**
     * Gets the blocked tasks for this DependencyInfo and marks them as "released"
     * @return
     */
    public synchronized Set<FragmentTaskMessage> getAndReleaseBlockedFragmentTaskMessages() {
        assert(this.blocked_tasks_released == false) : "Trying to unblock tasks more than once for txn #" + this.ts.txn_id;
        this.blocked_tasks_released = true;
        return (this.blocked_tasks);
    }
    
    /**
     * Add a partition id that we expect to return a result/response for this dependency
     * @param partition
     */
    public void addPartition(int partition) {
        this.partitions.add(partition);
    }
    
    /**
     * Add a response for a PartitionId
     * Returns true if we have also stored the result for this PartitionId
     * @param partition
     * @return
     */
    public synchronized boolean addResponse(int partition) {
        if (trace.get()) LOG.trace("Storing RESPONSE for DependencyId #" + this.dependency_id + " from Partition #" + partition + " in txn #" + this.ts.txn_id);
        assert(this.responses.contains(partition) == false);
        this.responses.add(partition);
        return (this.results.contains(partition));
    }
    
    /**
     * Add a result for a PartitionId
     * Returns true if we have also stored the response for this PartitionId
     * @param partition
     * @param result
     * @return
     */
    public synchronized boolean addResult(int partition, VoltTable result) {
        if (trace.get()) LOG.trace("Storing RESULT for DependencyId #" + this.dependency_id + " from Partition #" + partition + " in txn #" + this.ts.txn_id + " with " + result.getRowCount() + " tuples");
        assert(this.results.contains(partition) == false);
        this.results.add(partition);
        this.results_list.add(result);
        return (this.responses.contains(partition)); 
    }
    
    protected List<VoltTable> getResults() {
        return (this.results_list);
    }
    
    protected List<Integer> getResponses() {
        return (this.responses);
    }
    
    /**
     * Return just the first result for this DependencyInfo
     * This should only be called to get back the results for the final VoltTable of a query
     * @return
     */
    public VoltTable getResult() {
        assert(this.results.isEmpty() == false) : "There are no result available for " + this;
        assert(this.results.size() == 1) : "There are " + this.results.size() + " results for " + this + "\n-------\n" + this.getResults();
        return (this.results_list.get(0));
    }
    
    /**
     * Returns true if the task blocked by this Dependency is now ready to run 
     * @return
     */
    public boolean hasTasksReady() {
        int num_partitions = this.partitions.size();
        if (trace.get()) {
            LOG.trace("Block Tasks Not Empty? " + !this.blocked_tasks.isEmpty());
            LOG.trace("# of Results:   " + this.results.size());
            LOG.trace("# of Responses: " + this.responses.size());
            LOG.trace("# of <Responses/Results> Needed = " + num_partitions);
        }
        boolean ready = (this.blocked_tasks.isEmpty() == false) &&
                        (this.blocked_tasks_released == false) &&
                        (this.results.size() == num_partitions) &&
                        (this.responses.size() == num_partitions);
        return (ready);
    }
    
    public boolean hasTasksBlocked() {
        return (this.blocked_tasks.isEmpty() == false);
    }
    
    public boolean hasTasksReleased() {
        return (this.blocked_tasks_released);
    }
    
    @Override
    public String toString() {
        if (this.isInitialized() == false) {
            return ("<UNINITIALIZED>");
        }
        
        StringBuilder b = new StringBuilder();
        String status = null;
        int num_partitions = this.partitions.size();
        if (this.results.size() == num_partitions && this.responses.size() == num_partitions) {
            if (this.blocked_tasks_released == false) {
                status = "READY";
            } else {
                status = "RELEASED";
            }
        } else if (this.blocked_tasks.isEmpty()) {
            status = "WAITING";
        } else {
            status = "BLOCKED";
        }
        
        b.append("DependencyInfo[#").append(this.dependency_id).append("]\n")
         .append("  Partitions: ").append(this.partitions).append("\n")
         .append("  Responses:  ").append(this.responses.size()).append("\n")
         .append("  Results:    ").append(this.results.size()).append("\n")
         .append("  Blocked:    ").append(this.blocked_tasks).append("\n")
         .append("  Status:     ").append(status).append("\n");
        return b.toString();
    }

}