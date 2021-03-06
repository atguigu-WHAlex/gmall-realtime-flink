package com.atguigu.utils;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

public class KeyWordUtil {

    public static List<String> analyze(String keyWord) {

        //定义集合
        ArrayList<String> list = new ArrayList<>();

        //创建Reader
        StringReader reader = new StringReader(keyWord);

        IKSegmenter ikSegmenter = new IKSegmenter(reader, true);

        Lexeme next = null;
        try {
            next = ikSegmenter.next();

            while (next != null) {

                //将分出的词加入集合
                list.add(next.getLexemeText());
                next = ikSegmenter.next();
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

        //返回结果
        return list;

    }

    public static void main(String[] args) {
        System.out.println(analyze("尚硅谷大数据项目之Flink实时数仓"));
    }

}
