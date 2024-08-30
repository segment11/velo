package io.velo.command;

public class TestByteArrayOptSeq {
    public static void main(String[] args) {
        byte[] inData = {1, 2, 3, 4};
        int ip = 1;

        int refIndex = 0;
        refIndex -= inData[ip++];

        System.out.println(refIndex);
        System.out.println(ip);
    }
}
