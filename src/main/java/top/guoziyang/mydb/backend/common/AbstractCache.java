package top.guoziyang.mydb.backend.common;

import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import top.guoziyang.mydb.common.Error;

/**
 * AbstractCache 实现了一个引用计数策略的缓存
 * 提供了三个方法：
 * get(long key): 从缓存中获取 key 资源，并维护一个缓存器；
 * release(long key): 释放 key 缓存，依赖 releaseForCache() 方法将缓存写回数据源
 * close(): 关闭缓存器，依赖 release() 方法释放所有缓存
 * 定义了两个抽象方法：
 * releaseForCache(T obj): 当资源被驱逐时的写回行为
 * getForCache(long key): 当资源不在缓存时的获取行为
 */
public abstract class AbstractCache<T> {
    private HashMap<Long, T> cache;                 // key：缓存资源的标签（pgno或者UID），value：实际缓存的数据
    private HashMap<Long, Integer> references;      // key：缓存资源的标签，value：资源的引用个数
    private HashMap<Long, Boolean> getting;         // key：缓存资源的标签，value：是否有线程正在从数据源中获取该资源

    private int maxResource;                        // 缓存的最大缓存资源数
    private int count = 0;                          // 缓存中元素的个数
    private Lock lock;

    public AbstractCache(int maxResource) {
        this.maxResource = maxResource;
        cache = new HashMap<>();
        references = new HashMap<>();
        getting = new HashMap<>();
        lock = new ReentrantLock();
    }

    // 以下两个为抽象方法，留给具体的实现类去完成
    /**
     * 当资源不在缓存时的获取行为
     */
    protected abstract T getForCache(long key) throws Exception;

    /**
     * 当资源被驱逐时的写回行为
     */
    protected abstract void releaseForCache(T obj);

    /**
     * 获取资源
     *
     * @param key 目标缓存资源的页面号 pgno(pageCache) 或者 UID（DataItem）
     * @return 目标资源实体
     * 获取资源时，首先进入一个死循环，来无限尝试从缓存里获取。
     * 首先就需要检查这个时候是否有其他线程正在从数据源获取这个资源，如果有，就过会再来看看，
     * 当然如果资源在缓存中，就可以直接获取并返回了，记得要给资源的引用数 +1。
     * 否则，如果缓存没满的话，就在 getting 中注册一下，该线程准备从数据源获取资源了。
     */
    protected T get(long key) throws Exception {
        // 无限循环尝试从缓存中获取资源
        while (true) {
            lock.lock();
            // 判断是否有线程在数据源中获取 key 资源
            if (getting.containsKey(key)) {
                // key资源正在被某个线程在数据源中获取，所以肯定不在缓存中
                lock.unlock();
                // 等待 1s 后再重新循环判断
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    continue;
                }
                continue;
            }

            // key资源已经在缓存中，直接返回
            if (cache.containsKey(key)) {
                T obj = cache.get(key);
                references.put(key, references.get(key) + 1);   // 对该资源的引用计数加一
                lock.unlock();
                return obj;
            }

            // 资源没在缓存中，也没有其他线程正在获取，那么此线程尝试从数据源中获取该资源
            if (maxResource > 0 && count == maxResource) {
                // 设置缓存个数为 0 或者 缓存满了，抛出缓存已满异常
                lock.unlock();
                throw Error.CacheFullException;
            }
            // 缓存数+1
            count++;
            getting.put(key, true);     // 在getting中注册以下key，表示key资源此时有某个线程正在从数据源中获取
            lock.unlock();
            // 跳出循环，去完成从数据源获取 key 资源的操作
            break;
        }

        // 从数据源中获取资源，直接调用抽象方法 getForCache() 即可，获取完成或者发生异常记得从 getting 中删除 key 资源标记
        T obj = null;
        try {
            obj = getForCache(key);     // obj 是一个 Page 类型的数据页
        } catch (Exception e) {
            lock.lock();
            count--;
            getting.remove(key);
            lock.unlock();
            throw e;
        }

        // 获取完成记得讲资源放进缓存中，并且在getting中移除key资源标记
        lock.lock();
        getting.remove(key);
        cache.put(key, obj);    // 将 key资源和对应的资源实体obj放进缓存中
        references.put(key, 1); // key资源的引用计数初始化为1
        lock.unlock();

        return obj;
    }

    /**
     * 释放一个缓存
     * 但是必须满足 key 缓存的引用计数为 0，才将维护的 cache 缓存 Map 中移除 key 即可释放缓存
     * 再调用releaseForCache()方法，完成当缓存资源被驱逐时的写回行为，如果页面是脏页面就会进行回源操作
     *
     * @param key 缓存页面
     */
    protected void release(long key) {
        lock.lock();
        try {
            int ref = references.get(key) - 1;
            if (ref == 0) {
                T obj = cache.get(key);
                releaseForCache(obj);   // 当缓存资源被驱逐时的写回行为
                references.remove(key);
                cache.remove(key);
                count--;
            } else {
                references.put(key, ref);
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * 关闭缓存器，写回所有资源，其实就是将所有缓存释放掉
     */
    protected void close() {
        lock.lock();
        try {
            Set<Long> keys = cache.keySet();
            for (long key : keys) {
                T obj = cache.get(key);
                releaseForCache(obj);   // 调用释放缓存方法
                references.remove(key);
                cache.remove(key);
            }
        } finally {
            lock.unlock();
        }
    }
}
