package com.hadoop.project;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ElectricityMapper extends Mapper<Object, Text, Text, IntWritable> {

    private Text year = new Text();
    private IntWritable avgConsumption = new IntWritable();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Tách các trường bằng khoảng trắng hoặc tab
        String[] fields = value.toString().split("\\s+");
        if (fields.length == 13) {
            year.set(fields[0]); // Năm

            // Tính trung bình giá trị tiêu thụ điện
            int sum = 0;
            int count = 0;
            for (int i = 1; i < fields.length; i++) {
                try {
                    sum += Integer.parseInt(fields[i]);
                    count++;
                } catch (NumberFormatException e) {
                    // Bỏ qua lỗi nếu có
                }
            }

            int avg = sum / count;
            avgConsumption.set(avg);
            context.write(year, avgConsumption);
        }
    }
}
