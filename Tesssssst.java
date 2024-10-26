hdfs dfs -mkdir /user/cloudera/bai_toan_dien
hdfs dfs -put sample.txt /user/cloudera/bai_toan_dien

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ElectricityMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Tách dòng dữ liệu thành các phần
        String[] fields = value.toString().split("\\s+");
        
        // Lấy năm và mức tiêu thụ trung bình
        String year = fields[0];
        int avg = Integer.parseInt(fields[fields.length - 1]);
        
        // Đưa ra kết quả
        context.write(new Text(year), new IntWritable(avg));
    }
}


import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ElectricityReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        for (IntWritable value : values) {
            // Chỉ ghi các năm có mức tiêu thụ điện trung bình lớn hơn 30
            if (value.get() > 30) {
                context.write(key, value);
            }
        }
    }
}


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ElectricityDriver {
    public static void main(String[] args) throws Exception {
        // Kiểm tra tham số đầu vào
        if (args.length != 2) {
            System.err.println("Usage: ElectricityDriver <input path> <output path>");
            System.exit(-1);
        }
        
        // Cấu hình Job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Electricity Consumption");
        job.setJarByClass(ElectricityDriver.class);
        job.setMapperClass(ElectricityMapper.class);
        job.setReducerClass(ElectricityReducer.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        // Thiết lập đường dẫn input và output
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        // Chạy job
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}



javac -classpath `hadoop classpath` -d . ElectricityMapper.java ElectricityReducer.java ElectricityDriver.java



    jar -cvf ElectricityConsumption.jar *.class



    hadoop jar ElectricityConsumption.jar ElectricityDriver /user/cloudera/bai_toan_dien/sample.txt /user/cloudera/bai_toan_dien/output



    hdfs dfs -cat /user/cloudera/bai_toan_dien/output/part-r-00000
