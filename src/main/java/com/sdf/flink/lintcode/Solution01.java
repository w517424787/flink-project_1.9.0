package com.sdf.flink.lintcode;

import java.util.HashMap;
import java.util.Map;

/**
 * Modify by wwg 2020-05-30
 * 给定一个整数数组 nums 和一个目标值 target，
 * 请你在该数组中找出和为目标值的那两个整数，并返回他们的数组下标。
 * 给定 nums = [2, 7, 11, 15], target = 9
 * 因为 nums[0] + nums[1] = 2 + 7 = 9
 * 所以返回 [0, 1]
 * 思路：通过map的<key,value>格式来判断差值是否存在
 */
public class Solution01 {

    public static int[] twoSum(int[] nums, int target) {
        Map<Integer, Integer> map = new HashMap<>();
        int[] result = new int[2];
        for (int i = 0; i < nums.length; i++) {
            int diff = target - nums[i];
            if (map.containsKey(diff)) {
                result[0] = map.get(diff);
                result[1] = i;
                break;
            } else {
                map.put(nums[i], i);
            }
        }
        return result;
    }

    public static void main(String[] args) {
        int[] nums = {2, 7, 11, 15, 6};
        int target = 13;
        int[] result = twoSum(nums, target);
        for (int i : result) {
            System.out.println(i);
        }
    }
}
