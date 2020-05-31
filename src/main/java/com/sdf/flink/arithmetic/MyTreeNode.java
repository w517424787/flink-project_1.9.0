package com.sdf.flink.arithmetic;

import java.util.Arrays;
import java.util.LinkedList;

public class MyTreeNode {

    private static class TreeNode {
        int data;
        TreeNode leftChild;
        TreeNode rightChild;

        TreeNode(int data) {
            this.data = data;
        }
    }

    /**
     * 创建二叉树
     * 递归进行二叉树创建，根、左、右
     *
     * @param inputList
     * @return
     */
    public static TreeNode createBinaryTree(LinkedList<Integer> inputList) {
        if (inputList == null || inputList.isEmpty()) {
            return null;
        }
        TreeNode node = null;
        Integer data = inputList.removeFirst();
        if (data != null) {
            //先创建根节点
            node = new TreeNode(data);
            //在遍历根节点左边
            node.leftChild = createBinaryTree(inputList);
            //在遍历根节点右边
            node.rightChild = createBinaryTree(inputList);
        }
        return node;
    }

    /**
     * 前序遍历，根、左、右
     *
     * @param node
     */
    public static void preOrderTraversal(TreeNode node) {
        if (node != null) {
            System.out.println(node.data);
            //左树
            preOrderTraversal(node.leftChild);
            //右树
            preOrderTraversal(node.rightChild);
        }
    }

    /**
     * 中序遍历，左、根、右
     *
     * @param node
     */
    public static void midOrderTraversal(TreeNode node) {
        if (node != null) {
            //左树
            midOrderTraversal(node.leftChild);
            //根
            System.out.println(node.data);
            //右树
            midOrderTraversal(node.rightChild);
        }
    }

    /**
     * 后序遍历，左、右、根
     *
     * @param node
     */
    public static void rightOrderTraversal(TreeNode node) {
        if (node != null) {
            //左树
            rightOrderTraversal(node.leftChild);
            //右树
            rightOrderTraversal(node.rightChild);
            //根
            System.out.println(node.data);
        }
    }

    public static void main(String[] args) {
        LinkedList<Integer> linkedList = new LinkedList<Integer>
                (Arrays.asList(3, 2, 9, null, null, 10, null, null, 8, null, 4));
        //创建二叉树
        TreeNode node = createBinaryTree(linkedList);
        System.out.println("前序遍历：");
        preOrderTraversal(node);
        System.out.println("中序遍历：");
        midOrderTraversal(node);
        System.out.println("后序遍历：");
        rightOrderTraversal(node);
    }
}
