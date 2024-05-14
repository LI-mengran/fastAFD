package FastAFD;


import FastAFD.passjoin.SubstringableString;

import java.util.*;
import java.util.regex.Pattern;

public class Utils {
    static class ColumnWithSize implements Comparable<ColumnWithSize> {
        public long size;
        public int columnIndex;

        public ColumnWithSize(int columnIndex, long size) {
            this.columnIndex = columnIndex;
            this.size = size;
        }

        @Override
        public int compareTo(ColumnWithSize columnWithSize) {
            return (int) (columnWithSize.size - this.size);
        }
    }

    public static class SubstringInformation {
        public byte minPos;
        public byte maxPos;
        public byte segmentLength;

        public SubstringInformation(byte minPos, byte maxPos, byte segmentLength) {
            this.minPos = minPos;
            this.maxPos = maxPos;
            this.segmentLength = segmentLength;
        }
    }

    // inputLength -> queryLength -> segment -> substringInformation
    public static final List<HashMap<Integer, HashMap<Integer, ArrayList<SubstringInformation>>>> substringInfos = new ArrayList<>();
    public static int[][] editDistanceBuffer;

    public static Pattern delimiterPattern = Pattern.compile("\\s+");

    static public int[] getStartPositions(int length, int segmentCount) {
        int[] result = new int[segmentCount + 1];
        int longSegments = length % segmentCount;
        int shortSegments = segmentCount - longSegments;
        int segmentLength = length / segmentCount;
        for (int i = 0; i < shortSegments; i++) {
            result[i] = i * segmentLength;
        }
        int offset = shortSegments * segmentLength;
        segmentLength++;
        for (int i = 0; i < longSegments; i++) {
            result[i + shortSegments] = offset + i * segmentLength;
        }
        result[segmentCount] = length;
        return result;
    }

    static public int getStartPositionFromSegmentIndex(int segmentIndex, int stringLength, int segmentCount) {
        int longSegments = stringLength % segmentCount;
        int shortSegments = segmentCount - longSegments;
        int segmentLength = stringLength / segmentCount;
        int longSegmentOffset = segmentIndex - shortSegments;
        if (longSegmentOffset > 0) {
            return segmentIndex * segmentLength + longSegmentOffset;
        }
        return segmentIndex * segmentLength;
    }

    static public ArrayList<ArrayList<SubstringableString>> generateSubstrings(SubstringableString input, ArrayList<SubstringInformation> substringInformation, int segmentCount, int queriedLength) {
        ArrayList<ArrayList<SubstringableString>> result = new ArrayList<>(segmentCount);
        if (segmentCount > queriedLength) {
            ArrayList<SubstringableString> allCharacters = new ArrayList<>(input.length());
            for (int i = 0; i < input.length(); i++) {
                allCharacters.add(input.substring(i, i + 1));
            }
            for (int i = 0; i < segmentCount - queriedLength; i++) {
                result.add(new ArrayList<>(0));
            }
            for (int i = segmentCount - queriedLength; i < segmentCount; i++) {
                result.add(allCharacters);
            }
        } else {
            for (SubstringInformation s : substringInformation) {
                ArrayList<SubstringableString> curr = new ArrayList<>();
                for (int j = s.minPos; j <= s.maxPos; j++) {
                    curr.add(input.substring(j - 1, j - 1 + s.segmentLength));
                }
                result.add(curr);
            }
        }
        return result;
    }

    static public ArrayList<SubstringInformation> generateSubstringInformation(int segmentCount, int inputLength, int queriedLength) {
        if (segmentCount > queriedLength) return new ArrayList<>(0);

        ArrayList<SubstringInformation> result = new ArrayList<>(segmentCount);
        int lengthDifference = inputLength - queriedLength;

        int startPosition = 1;
        int segmentLength = queriedLength / segmentCount;
        int shortSegments = segmentCount - (queriedLength % segmentCount);
        for (int i = 1; i <= segmentCount; i++) {
            int minL = startPosition - i + 1;
            int minR = startPosition + lengthDifference - (segmentCount - 1 + 1 - i);
            int minPos = Math.max(1, Math.max(minL, minR));

            int maxL = startPosition + i - 1;
            int maxR = startPosition + lengthDifference + (segmentCount - 1 + 1 - i);
            int maxPos = Math.min(inputLength - segmentLength + 1, Math.min(maxL, maxR));

            result.add(new SubstringInformation((byte) minPos, (byte) maxPos, (byte) segmentLength));

            startPosition += segmentLength;

            if (i == shortSegments)
                segmentLength++;
        }
        return result;
    }

    // Inspired by the EditDistanceClusterer (https://github.com/lispc/EditDistanceClusterer) - Copyright (c) [2015] [Zhuo Zhang]
    static public boolean isWithinEditDistance(SubstringableString src, SubstringableString dst, int srcMatchPos, int dstMatchPos, int len, int threshold,
                                               int[][] distanceBuffer, int[] editDistance) {
        int srcRightLen = src.length() - srcMatchPos - len;
        int dstRightLen = dst.length() - dstMatchPos - len;
        int leftThreshold = threshold - Math.abs(srcRightLen - dstRightLen);
        int leftDistance = calculateEditDistanceWithThreshold(src, 0, srcMatchPos,
                dst, 0, dstMatchPos,
                leftThreshold, distanceBuffer);
        if (leftDistance > leftThreshold) {
            return false;
        }
        int rightThreshold = threshold - leftDistance;
        int rightDistance = calculateEditDistanceWithThreshold(
                src, srcMatchPos + len, src.length() - srcMatchPos - len,
                dst, dstMatchPos + len, dst.length() - dstMatchPos - len,
                rightThreshold, distanceBuffer);
        editDistance[0] = leftDistance + rightDistance;
        return rightDistance <= rightThreshold;
    }


    // Inspired by the EditDistanceClusterer (https://github.com/lispc/EditDistanceClusterer) - Copyright (c) [2015] [Zhuo Zhang]
    static public Integer calculateEditDistanceWithThreshold(SubstringableString s1, int start1, int l1,
                                                         SubstringableString s2, int start2, int l2, int threshold, int[][] distanceBuffer) {
        if (threshold < 0) {
            return 0;
        }
        if (threshold == 0) {
            SubstringableString sub1 = s1.substring(start1, start1 + l1);
            SubstringableString sub2 = s2.substring(start2, start2 + l2);
            return sub1.equals(sub2) ? 0 : 1;
        }
        if (l1 == 0) {
            return l2;
        }
        if (l2 == 0) {
            return l1;
        }

        if(Math.abs(l1 - l2) > threshold) return threshold + 1;
        char[] s1_chars = s1.getUnderlyingChars();
        char[] s2_chars = s2.getUnderlyingChars();
        for (int j = 1; j <= l1; j++) {
            int start = Math.max(j - threshold, 1);
            int end = Math.min(l2, j + threshold);
            if (j - threshold - 1 >= 1) {
                distanceBuffer[j - threshold - 1][j] = threshold + 1;
            }

            for (int i = start; i <= end; i++) {
                if (s1_chars[start1 + j - 1] == s2_chars[start2 + i - 1]) {
                    distanceBuffer[i][j] = distanceBuffer[i - 1][j - 1];
                } else {
                    distanceBuffer[i][j] = Math.min(distanceBuffer[i - 1][j - 1] + 1,
                            Math.min(distanceBuffer[i - 1][j] + 1, distanceBuffer[i][j - 1] + 1));
                }
            }
            if (end < l2)
                distanceBuffer[end + 1][j] = threshold + 1;
            boolean earlyTerminateFlag = true;
            for (int i = start; i <= end; i++) {
                if (distanceBuffer[i][j] <= threshold) {
                    earlyTerminateFlag = false;
                    break;
                }
            }
            if (earlyTerminateFlag)
                return threshold + 1;
        }
        return distanceBuffer[l2][l1];
    }

    public static Integer calculateEditDistance(String pointA, String pointB){
            int len1 = pointA.length();
            int len2 = pointB.length();
//            if(Math.abs(len1 - len2) > threshold)return Math.abs(len1 - len2);
            int [][] dp = new int[len1 + 1][len2 + 1];
            for(int i = 0; i <= len1; i ++){
                dp[i][0] = i;
            }
            for(int j = 0; j <= len2; j ++){
                dp[0][j] = j;
            }
            for(int i = 1; i <= len1; i ++) {
//                boolean earlyTerminateFlag = true;
                for (int j = 1; j <= len2; j++) {
                    int d = 1;
                    if(pointA.charAt(i - 1) == pointB.charAt(j - 1)){
                        d = 0;
                    }
                    dp[i][j] = Math.min(dp[i - 1][j - 1] + d, Math.min(dp[i - 1][j] + 1, dp[i][j - 1] + 1));
//                    if(dp[i][j] <= threshold)
//                        earlyTerminateFlag = false;
                }
            }
            return dp[len1][len2];
        }

    public static BitSet listToBitSet(List<Integer> list, int maxValue) {
        BitSet bitSet = new BitSet(list.size() * maxValue + 1); // 创建一个BitSet，大小为最大值+1

        // 将列表中的整数转换为BitSet中的位设置为true
        for (int index = 0; index < list.size(); index ++) {
            if (list.get(index) >= 0 && list.get(index) <= maxValue) {
                bitSet.set(index * maxValue + list.get(index));
            }
        }

        return bitSet;
    }

//    public static List<Integer> listToBitSet(BitSet bitSet, int maxValue) {
//        List<Integer> list = new ArrayList<>(); // 创建一个BitSet，大小为最大值+1
//
//        // 将列表中的整数转换为BitSet中的位设置为true
//        for (int index = 0; index < list.size(); index ++) {
//            if (list.get(index) >= 0 && list.get(index) <= maxValue) {
//                bitSet.set(index * maxValue + list.get(index));
//            }
//        }
//
//        return bitSet;
//    }
}
