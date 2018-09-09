package com.threecube.phoenix;

import com.threecube.phoenix.writer.bean.HbaseFileWriter;
import com.threecube.phoenix.writer.bean.HbaseViewCreator;
import com.threecube.phoenix.writer.bean.JsonFileWriter;
import com.threecube.phoenix.writer.util.ConstentsUtil;

/**
 * Created by Mike Ding on 2015/12/8.
 */
public class LogWriterStarter {
    public static void main(String[] args){

        long startTime = System.currentTimeMillis();
        HbaseFileWriter writer = new HbaseFileWriter();
        writer.serJsonFileWriter(new JsonFileWriter(true));
        writer.setHbaseViewCreator(new HbaseViewCreator(true));
        writer.run(ConstentsUtil.hdfsPath);
        long endTime = System.currentTimeMillis();
        System.out.println("#### time : " + (endTime - startTime));

    }
}
