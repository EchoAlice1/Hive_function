package com.fmg.hive.udtf;


import com.google.gson.Gson;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.json.JSONArray;

import java.util.ArrayList;
import java.util.List;

public class ExplodeJSONArray extends GenericUDTF {


    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
        // 1 参数合法性检查
        if(argOIs.getAllStructFieldRefs().size()!=1){
            throw new UDFArgumentException("ExplodeJSONArray 只需要一个参数"); }
        // 2 第一个参数必须为 string
        if (!"string".equals(argOIs.getAllStructFieldRefs().get(0))){
         throw new UDFArgumentException("json_array_to_struct_array 的第 1 个参数应为string 类型");
        }
        // 3 定义返回值名称和类型
        List<String> fieldNames = new ArrayList<String>();
        List<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
        fieldNames.add("items");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames,fieldOIs);
    }


    @Override
    public void process(Object[] objects) throws HiveException {
        // 1 获取传入的数据
        String jsonArray = objects[0].toString();
        // 2 将 string 转换为 json 数组
        JSONArray actions = new JSONArray(jsonArray);
        // 3 循环一次，取出数组中的一个 json，并写出
        for (int i =0;i< actions.length();i++){
            String[] result =new String[1];
            result[0] = actions.getString(i);
            //forward方法：每调用一次，就输出一行新的结果数据
            forward(result);
        }
    }

    @Override
    public void close() throws HiveException {
    }
}
