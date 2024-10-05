import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ElectricityMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Tách từng dòng dữ liệu
        String[] fields = value.toString().split(" ");
        String year = fields[0]; // Năm
        int avgElectricity = Integer.parseInt(fields[fields.length - 1]); // Lấy giá trị trung bình

        // Đưa cặp (year, avgElectricity) vào context
        context.write(new Text(year), new IntWritable(avgElectricity));
    }
}
