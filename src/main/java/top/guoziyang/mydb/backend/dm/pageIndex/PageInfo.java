package top.guoziyang.mydb.backend.dm.pageIndex;

/**
 * 页面信息数据结构：页号 和 空闲大小
 * PageInfo 实际保存了某页的页号，和该页空闲的区间大小。
 */
public class PageInfo {
    public int pgno;
    public int freeSpace;

    public PageInfo(int pgno, int freeSpace) {
        this.pgno = pgno;
        this.freeSpace = freeSpace;
    }
}