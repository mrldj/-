package my.hadoopstudy.mapreduce;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import train.Duplicate.Map;
import train.Duplicate.Reduce;

/**
 * 升序排序（使用mapreduce提供的默认排序规则）
 * 对于IntWritable类型的数据，按key值大小进行排序
 * @author hadoop
 *
 */
public class Sort {
    //将输入数据的value装换为int类型并作为key输出
    public static class Map extends Mapper<Object,Text,IntWritable,IntWritable>{
        private static IntWritable numble = new IntWritable();
        private static final IntWritable one = new IntWritable(1);
        public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
            String line  = value.toString();
            numble.set(Integer.parseInt(line));
            context.write(numble,  one);
        }
    }
    //全局num确定每个数字的顺序位次
    //遍历values来确定每个数字输出的次数
    public static class Reduce extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable>{
        private static IntWritable num = new IntWritable(1);
        public void reduce(IntWritable key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException{

            //System.out.println(key+"  "+num);
            for(IntWritable value:values){
                context.write(num, key);
                System.out.println(key+"--"+value+"--"+num);
            }
            num = new IntWritable(num.get()+1);
        }
    }
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
          Configuration conf = new Configuration();  
            conf.set("mapred.job.tracker", "localhost:9001");
            args = new String[]{"hdfs://localhost:9000/user/hadoop/input/sort_in","hdfs://localhost:9000/user/hadoop/output/sort_out"};
            //检查运行命令  
            String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();  
            if(otherArgs.length != 2){  
                System.err.println("Usage Sort <int> <out>");  
                System.exit(2);  
            }  
            //配置作业名  
            Job job = new Job(conf,"sort");  
            //配置作业各个类  
            job.setJarByClass(Sort.class);  
            job.setMapperClass(Map.class);  
            job.setReducerClass(Reduce.class);  
            job.setOutputKeyClass(IntWritable.class);  
            job.setOutputValueClass(IntWritable.class);  
            FileInputFormat.addInputPath(job, new Path(otherArgs[0]));  
            FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));  
            System.exit(job.waitForCompletion(true) ? 0 : 1);  

    }

}