package top.guoziyang.mydb.backend.vm;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import top.guoziyang.mydb.common.Error;

/**
 * 表锁：
 * 维护了一个依赖等待图，以进行死锁检测
 */
public class LockTable {

    private Map<Long, List<Long>> x2u;  // 某个XID已经获得的资源的UID列表,键是事务ID，值是该事物持有的资源ID列表
    private Map<Long, Long> u2x;        // UID被某个XID持有,键是资源ID，值是持有该资源的事务ID。
    private Map<Long, List<Long>> wait; // 正在等待UID的XID列表
    private Map<Long, Lock> waitLock;   // 正在等待资源的XID的锁
    private Map<Long, Long> waitU;      // XID正在等待的UID
    private Lock lock;                  // 一个全局锁，用于同步
    private Map<Long, Integer> xidStamp;
    private int stamp;

    public LockTable() {
        x2u = new HashMap<>();
        u2x = new HashMap<>();
        wait = new HashMap<>();
        waitLock = new HashMap<>();
        waitU = new HashMap<>();
        lock = new ReentrantLock();
    }

    /**
     * 向依赖等待图中添加一个等待记录
     * 事务xid 阻塞等待 数据项uid，如果会造成死锁则抛出异常
     * @param xid 事务id
     * @param uid 数据项key
     * @return 不需要等待则返回null，否则返回锁对象
     * @throws Exception
     */
    //在每次出现等待的情况时，就尝试向图中增加一条边，并进行死锁检测。如果检测到死锁，就撤销这条边，不允许添加，并撤销该事务。
    // 不需要等待则返回null，否则返回锁对象或者会造成死锁则抛出异常
    public Lock add(long xid, long uid) throws Exception {
        lock.lock();
        try {
            // dataitem数据已经被事务xid获取到，不需要等待，返回null
            if(isInList(x2u, xid, uid)) {
                return null;
            }

            // 检查UID资源是否已经被其他XID事务持有
            if(!u2x.containsKey(uid)) {
                u2x.put(uid, xid);                  // 如果没有被持有，将资源分配给当前事务
                putIntoList(x2u, xid, uid);         // 将资源添加到事务的资源列表中
                return null;
            }

            // 如果资源已经被其他事务持有，将当前事务添加到等待列表中
            waitU.put(xid, uid);
            putIntoList(wait, xid, uid);

            // 检查是否存在死锁
            if(hasDeadLock()) {
                waitU.remove(xid);                  // 如果存在死锁，从等待列表中移除当前事务
                removeFromList(wait, uid, xid);
                throw Error.DeadlockException;      // 抛出死锁异常
            }

            // 如果不存在死锁，为当前事务创建一个新的锁，并锁定它
            Lock l = new ReentrantLock();
            l.lock();
            waitLock.put(xid, l);                   // 将新的锁添加到等待锁列表中
            return l;                               // 返回新的锁

        } finally {
            lock.unlock();                          // 解锁全局锁
        }
    }

    /**
     *当一个事务commit或者abort时，就会释放掉它自己持有的锁，并将自身从等待图中删除
     * @param xid
     */
    public void remove(long xid) {
        lock.lock();                                // 获取全局锁
        try {
            List<Long> l = x2u.get(xid);            // 从x2u映射中获取当前事务ID已经获得的资源的UID列表
            if(l != null) {
                while(l.size() > 0) {
                    Long uid = l.remove(0);   // 获取并移除列表中的第一个资源ID
                    selectNewXID(uid);              // 从等待队列中选择一个新的事务ID来占用这个资源
                }
            }
            waitU.remove(xid);                      // 从waitU映射中移除当前事务ID
            x2u.remove(xid);                        // 从x2u映射中移除当前事务ID
            waitLock.remove(xid);                   // 从waitLock映射中移除当前事务ID

        } finally {
            lock.unlock();                          // 解锁全局锁
        }
    }

    // 从等待队列中选择一个xid来占用uid
    private void selectNewXID(long uid) {
        u2x.remove(uid);                            // 从u2x映射中移除当前资源ID
        List<Long> l = wait.get(uid);               // 从wait映射中获取当前资源ID的等待队列
        if (l == null) return;                      // 如果等待队列为空，立即返回
        assert l.size() > 0;                        // 断言等待队列不为空

        // 遍历等待队列
        while (l.size() > 0) {
            long xid = l.remove(0);          // 获取并移除队列中的第一个事务ID
            // 检查事务ID是否在waitLock映射中
            if (!waitLock.containsKey(xid)) {
                continue;                          // 如果不在，跳过这个事务ID，继续下一个
            } else {
                u2x.put(uid, xid);                  // 将事务ID和资源ID添加到u2x映射中
                Lock lo = waitLock.remove(xid);     // 从waitLock映射中移除这个事务ID
                waitU.remove(xid);                  // 从waitU映射中移除这个事务ID
                lo.unlock();                        // 解锁这个事务ID的锁
                break;                              // 跳出循环
            }
        }
        // 如果等待队列为空，从wait映射中移除当前资源ID
        if (l.size() == 0) wait.remove(uid);
    }

    // 死锁检测
    private boolean hasDeadLock() {
        xidStamp = new HashMap<>();                  // 创建一个新的xidStamp哈希映射
        stamp = 1;
        for(long xid : x2u.keySet()) {               // 遍历所有已经获得资源的事务ID
            Integer s = xidStamp.get(xid);           // 获取xidStamp中对应事务ID的记录
            if(s != null && s > 0) {                 // 如果记录存在，并且值大于0
                continue;                            // 跳过这个事务ID，继续下一个
            }
            stamp++;
            if(dfs(xid)) {                           // 调用dfs方法进行深度优先搜索
                return true;                         // 如果dfs方法返回true，表示存在死锁，那么hasDeadLock方法也返回true
            }
        }
        return false;
    }

    private boolean dfs(long xid) {
        Integer stp = xidStamp.get(xid);            // 从xidStamp映射中获取当前事务ID的时间戳
        if(stp != null && stp == stamp) {           // 如果时间戳存在并且等于全局时间戳
            return true;                            // 存在死锁，返回true
        }
        if(stp != null && stp < stamp) {            // 如果时间戳存在并且小于全局时间戳
            return false;                           // 这个事务ID已经被检查过，并且没有发现死锁，返回false
        }
        xidStamp.put(xid, stamp);                   // 将当前事务ID和全局时间戳添加到xidStamp映射中

        Long uid = waitU.get(xid);                  // 从waitU映射中获取当前事务ID正在等待的资源ID
        if (uid == null) return false;              // 如果资源ID不存在，表示当前事务ID不在等待任何资源，返回false
        Long x = u2x.get(uid);                      // 从u2x映射中获取当前资源ID被哪个事务ID持有
        assert x != null;                           // 断言这个事务ID存在
        return dfs(x);                              // 递归调用dfs方法检查这个事务ID
    }

    private void removeFromList(Map<Long, List<Long>> listMap, long uid0, long uid1) {
        List<Long> l = listMap.get(uid0);
        if(l == null) return;
        Iterator<Long> i = l.iterator();
        while(i.hasNext()) {
            long e = i.next();
            if(e == uid1) {
                i.remove();
                break;
            }
        }
        if(l.size() == 0) {
            listMap.remove(uid0);
        }
    }

    private void putIntoList(Map<Long, List<Long>> listMap, long uid0, long uid1) {
        if(!listMap.containsKey(uid0)) {
            listMap.put(uid0, new ArrayList<>());
        }
        listMap.get(uid0).add(0, uid1);
    }

    private boolean isInList(Map<Long, List<Long>> listMap, long uid0, long uid1) {
        List<Long> l = listMap.get(uid0);
        if(l == null) return false;
        Iterator<Long> i = l.iterator();
        while(i.hasNext()) {
            long e = i.next();
            if(e == uid1) {
                return true;
            }
        }
        return false;
    }

}
