package top.guoziyang.mydb.backend.dm.pageIndex;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import top.guoziyang.mydb.backend.dm.pageCache.PageCache;

/**
 * 页面索引，缓存了每一页的空闲空间信息。
 * 实现原理：
 *      维护一个41大小的List代表不同空闲大小所在的区间，每个List[i] 拉出一条List存在这个区间上有空闲空间的所有页面信息；
 *      数据库启动的时候，先遍历所有页面信息，获取每个数据页空闲空间的大小并记录在页面索引中。
 * 目的：
 *      用于在上层模块进行插入操作时，能够快速找到一个合适空间的页面，而无需从磁盘或者缓存中检查每一个页面的信息。
 */
public class PageIndex {
    // 将一页划成40个区间
    private static final int INTERVALS_NO = 40;
    // 每个区间的内存大小
    private static final int THRESHOLD = PageCache.PAGE_SIZE / INTERVALS_NO;

    private Lock lock;
    // 维护一个页面信息的 List数组，实现页面索引
    // lists 是一个数组，数组中的每一个元素是一个 ArrayList 列表， 每个列表中保存了多个 PageInfo
    // lists 中第 i 个元素表示的是空余容量还剩 i 个区间的页面的集合
    private List<PageInfo>[] lists;

    @SuppressWarnings("unchecked")
    public PageIndex() {
        lock = new ReentrantLock();
        lists = new List[INTERVALS_NO+1];
        for (int i = 0; i < INTERVALS_NO+1; i++) {
            lists[i] = new ArrayList<>();
        }
    }

    /**
     * 从 PageIndex 中获取页面也很简单，算出区间号，直接取即可
     * @param spaceSize 需要的空闲空间大小
     * @return 第一个满足的页面信息
     */
    public PageInfo select(int spaceSize) {
        lock.lock();
        try {
            // 计算出满足请求空间的区间号
            int number = spaceSize / THRESHOLD;
            // 因为区间从1开始，所以要加1操作
            if(number < INTERVALS_NO) number++;
            // 循环查找大于等于请求空间的页面
            while(number <= INTERVALS_NO) {
                if(lists[number].size() == 0) {
                    // 当前区间没有空余页面，后移一个区间
                    number++;
                    continue;
                }
                // 从页面索引List中移除第一个满足的页面信息PageInfo
                return lists[number].remove(0);
            }
            return null;
        } finally {
            lock.unlock();
        }
    }

    /**
     * 插入页面操作
     * 前面被选择的页，会直接从 PageIndex 中移除，这意味着，同一个页面是不允许并发写的。
     * 在上层模块使用完这个页面后，需要将其重新插入 PageIndex
     * @param pgno 页号
     * @param freeSpace 空闲空间大小
     */
    public void add(int pgno, int freeSpace) {
        lock.lock();
        try {
            // 计算空闲空间的区间号
            int number = freeSpace / THRESHOLD;
            // 插入页面索引列表
            lists[number].add(new PageInfo(pgno, freeSpace));
        } finally {
            lock.unlock();
        }
    }
}
