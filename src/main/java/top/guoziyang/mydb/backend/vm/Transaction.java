package top.guoziyang.mydb.backend.vm;

import java.util.HashMap;
import java.util.Map;

import top.guoziyang.mydb.backend.tm.TransactionManagerImpl;

// vm对其他模块提供的一个抽象的事务数据结构
public class Transaction {
    public long xid;                    // 事务id
    public int level;                   // 事务隔离等级，0：读已提交；1：可重复读
    public Map<Long, Boolean> snapshot; // 活跃事务的快照，用于存储活跃事务的ID,用于实现可重复读
    public Exception err;               // 事务执行过程中的错误
    public boolean autoAborted;         // 自动回滚标记,标志事务是否自动中止

    // 创建一个新的事务
    public static Transaction newTransaction(long xid, int level, Map<Long, Transaction> active) {
        Transaction t = new Transaction();
        t.xid = xid;
        t.level = level;
        // 只有可重复读才需要 活跃事务列表
        // 如果隔离级别不为0，创建快照
        if(level != 0) {
            t.snapshot = new HashMap<>();
            // 将活跃事务的ID添加到快照中
            for(Long x : active.keySet()) {
                t.snapshot.put(x, true);
            }
        }
        return t;       // 返回新创建的事务
    }

    // 判断xid是否是活跃事务
    public boolean isInSnapshot(long xid) {
        if(xid == TransactionManagerImpl.SUPER_XID) {
            return false;                       // 如果事务ID等于超级事务ID，返回false
        }
        return snapshot.containsKey(xid);       // 否则，检查事务ID是否在快照中
    }
}
