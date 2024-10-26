Để hướng dẫn bạn từ đầu trong việc thiết lập dự án Java MapReduce và xây dựng nó trên Cloudera, mình sẽ đưa ra các bước tuần tự như sau:

### Mục tiêu:
- Tạo một chương trình Java MapReduce để đọc dữ liệu từ file `sample.txt`.
- Chạy chương trình trên Cloudera để xử lý dữ liệu.

### Các bước thực hiện:

---

### **1. Chuẩn bị dự án trong Eclipse**

1. **Tạo một dự án Java mới**:
   - Mở Eclipse và tạo một Java Project mới bằng cách vào `File` -> `New` -> `Java Project`.
   - Đặt tên cho Project, ví dụ: `ElectricityConsumption`.

2. **Tạo các Package và Class cần thiết**:
   - Tạo một package mới bên trong `src`, ví dụ: `com.hadoop.electricity`.
   - Tạo 3 file Java trong package này:
     - `ElectricityMapper.java`
     - `ElectricityReducer.java`
     - `ElectricityDriver.java`

3. **Viết mã cho các class trong dự án**:

   **ElectricityMapper.java**
   ```java
   package com.hadoop.electricity;

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
   ```

   **ElectricityReducer.java**
   ```java
   package com.hadoop.electricity;

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
   ```

   **ElectricityDriver.java**
   ```java
   package com.hadoop.electricity;

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
   ```

---

### **2. Thiết lập build path**

1. **Thêm các thư viện Hadoop**:
   - Nhấp chuột phải vào dự án của bạn trong Eclipse -> `Build Path` -> `Configure Build Path`.
   - Thêm các JAR thư viện Hadoop cần thiết (ví dụ: `hadoop-common.jar`, `hadoop-mapreduce-client-core.jar`...) từ thư mục Hadoop trên máy chủ Cloudera.

---

### **3. Tạo file dữ liệu mẫu `sample.txt`**

- Tạo file `sample.txt` và lưu trữ nó trong thư mục dự án của bạn.
- Nội dung ví dụ cho `sample.txt`:
  ```
  1979 23 25 2 43 24 25 25 26 25 26 26 25 29
  1980 26 27 32 28 30 24 31 31 30 31 30 29
  1981 30 31 36 39 33 31 39 41 38 38 36 34
  1984 39 38 40 39 41 43 41 40 41 39 38 40
  1985 41 39 39 40 39 43 41 41 40 41 40 45
  ```

---

### **4. Tạo file JAR**

1. **Xuất JAR từ Eclipse**:
   - Nhấp chuột phải vào dự án -> `Export` -> `JAR file`.
   - Chọn các file và lớp cần thiết, đặt tên file JAR (ví dụ: `ElectricityConsumption.jar`), và lưu.

---

### **5. Tải dữ liệu lên HDFS**

1. **Mở Terminal trong Cloudera** và tạo thư mục trên HDFS:
   ```bash
   hdfs dfs -mkdir /user/cloudera/bai_toan_dien
   ```

2. **Đưa tệp `sample.txt` lên HDFS**:
   ```bash
   hdfs dfs -put /home/cloudera/sample.txt /user/cloudera/bai_toan_dien
   ```

3. **Kiểm tra tệp trên HDFS**:
   ```bash
   hdfs dfs -ls /user/cloudera/bai_toan_dien
   ```

---

### **6. Chạy chương trình trên Cloudera**

1. **Thực thi chương trình Java**:
   ```bash
   hadoop jar ElectricityConsumption.jar com.hadoop.electricity.ElectricityDriver /user/cloudera/bai_toan_dien/sample.txt /user/cloudera/bai_toan_dien/output
   ```

2. **Kiểm tra kết quả**:
   ```bash
   hdfs dfs -cat /user/cloudera/bai_toan_dien/output/part-r-00000
   ```

---

### **7. Xử lý các vấn đề phát sinh**

- Nếu bạn gặp lỗi hoặc chương trình không chạy đúng, hãy kiểm tra các bước sau:
  - **Đảm bảo file `sample.txt` đã được tải lên HDFS**.
  - **Kiểm tra cấu trúc và định dạng của file `sample.txt`**.
  - **Đảm bảo rằng đường dẫn đầu vào và đầu ra trong lệnh `hadoop jar` là chính xác**.
  - **Xóa thư mục đầu ra nếu nó đã tồn tại trước đó** với lệnh: `hdfs dfs -rm -r /user/cloudera/bai_toan_dien/output`.

Với các bước trên, bạn sẽ có thể xây dựng và thực thi thành công chương trình MapReduce bằng Java trên môi trường Cloudera. Nếu bạn có bất kỳ câu hỏi nào khác, đừng ngần ngại cho mình biết nhé!
