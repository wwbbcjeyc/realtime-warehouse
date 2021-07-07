package com.zjtd.app.func;

import com.zjtd.utils.KeywordUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.List;


@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
public class KeywordTableFunction extends TableFunction<Row> {

    public void eval(String keyword) {

        //切词
        List<String> words = KeywordUtil.splitKeyword(keyword);

        //遍历写出
        for (String word : words) {
            collect(Row.of(word));
        }
    }
}
