package com.pgman.goku.nowcoder.base;


/**
 * 链表节点数据结构
 */
public class ListNode {
    public int val;
    public ListNode next = null;

    public ListNode(int val) {
        this.val = val;
    }

    @Override
    public String toString() {
        return this.val + (null == next ? "" : ", " + next);
    }
}
