package com.atguigu.app.func;

import com.atguigu.utils.KeyWordUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.List;

@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
public class KeyWordUDTF extends TableFunction<Row> {

    public void eval(String str) {

        //使用IK分词器对搜索的关键词进行分词处理
        List<String> list = KeyWordUtil.analyze(str);

        //遍历单词写出
        for (String word : list) {
            collect(Row.of(word));
        }
    }
}
