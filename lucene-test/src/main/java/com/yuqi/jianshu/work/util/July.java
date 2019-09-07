package com.yuqi.jianshu.work.util;

/**
 * Author yuqi
 * Time 31/8/19
 **/
public class July {

    //
    public LinkNode reverseKLinkNode(LinkNode root) {
        //
        return null;
    }

    public LinkNode reverseNode(LinkNode node) {
        if (node == null) {
            return null;
        }

        LinkNode p = node;
        LinkNode res = p;
        res.setNext(null);

        while (p.getNext() != null) {
            LinkNode n = p.getNext();
            n.setNext(res);
            res = n;

            p = p.getNext();
        }

        return res;
    }
}
