package com.sdf.flink.leetcode;

import java.util.HashMap;
import java.util.Map;

/**
 * Modify by wwg 2020-06-01
 * 给定一个字符串，请你找出其中不含有重复字符的 最长子串 的长度
 * 示例：
 * 输入: "abcabcbb"
 * 输出: 3
 * 解释: 因为无重复字符的最长子串是 "abc"，所以其长度为 3。
 */

public class Solution03 {

    /**
     * 查找无重复字符串最大长度
     *
     * @param s 字符串
     * @return
     */
    public static int lengthOfLongestSubstring(String s) {
        int n = s.length();
        int ans = 0;
        Map<Character, Integer> map = new HashMap<>();
        for (int j = 0, i = 0; j < n; j++) {
            if (map.containsKey(s.charAt(j))) {
                i = Math.max(map.get(s.charAt(j)), i);
            }

            ans = Math.max(ans, j - i + 1);
            map.put(s.charAt(j), j + 1);
        }

        return ans;
    }

    public static void main(String[] args) {
        String str = "dvdf";
        System.out.println(lengthOfLongestSubstring(str));
        //String str2 = "bbbbb";
        //System.out.println(lengthOfLongestSubstring(str2));
        //String str3 = "pwwkew";
        //System.out.println(lengthOfLongestSubstring(str3));
    }
}
