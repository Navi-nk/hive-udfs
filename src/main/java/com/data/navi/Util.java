package com.data.navi;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;

public class Util {

    public static void throwError(String message) throws UDFArgumentException{
        throw new UDFArgumentException(message);
    }
}
