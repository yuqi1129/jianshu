package com.yuqi.jianshu.work.util;

/**
 * Author yuqi
 * Time 31/8/19
 **/
public class LinkNode {
    private int value;
    private LinkNode next;

    public LinkNode(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    public LinkNode getNext() {
        return next;
    }

    public void setValue(int value) {
        this.value = value;
    }

    public void setNext(LinkNode next) {
        this.next = next;
    }
}
