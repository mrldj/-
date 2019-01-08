package my.hadoopstudy.mapreduce;

import java.io.IOException;
import java.util.StringTokenizer;

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


/**
 * ����ƽ����
 * @author hadoop
 *
 */
public class Average1 {

    public static class Map extends Mapper<Object,Text,IntWritable,IntWritable>{
        private static IntWritable no = new IntWritable(1);  //������Ϊkey
        private Text number = new Text();  //�洢���µ�����
        public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
            StringTokenizer st = new StringTokenizer(value.toString());
            while(st.hasMoreTokens()){
                number.set(st.nextToken());
                context.write(no, new IntWritable(Integer.parseInt(number.toString())));
            }
        }
    }
    public static class Reduce extends Reducer<IntWritable,IntWritable,Text,IntWritable>{
        //����ȫ�ֱ���
        int count = 0;   //���ֵ�����
        int sum = 0;     //���ֵ��ܺ�
        int max = -2147483648;
        int min = 2147483647;
        public void reduce(IntWritable key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException{
            for(IntWritable val:values){
                if(val.get()>max){
                    max = val.get();
                }
                if(val.get()<min){
                    min = val.get();
                }
                count++;
                sum+=val.get();
            }
            int average = (int)sum/count;  //����ƽ����
            //System.out.println(sum+"--"+count+"--"+average);
            context.write(new Text("ƽ����"), new IntWritable(average));
            context.write(new Text("���ֵ"), new IntWritable(max));
            context.write(new Text("��Сֵ"), new IntWritable(min));
        }
    }
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
         // TODO Auto-generated method stub  
        Configuration conf = new Configuration();  
        //conf.set("mapred.job.tracker", "localhost:9001");
        conf.addResource("config.xml");
        args = new String[]{"hdfs://localhost:9000/user/hadoop/input/average1_in","hdfs://localhost:9000/user/hadoop/output/average1_out"};
        //�����������  
        String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();  
        if(otherArgs.length != 2){  
            System.err.println("Usage WordCount <int> <out>");  
            System.exit(2);  
        }  
        //������ҵ��  
        Job job = new Job(conf,"average1 ");  
        //������ҵ������  
        job.setJarByClass(Average1.class);  
        job.setMapperClass(Map.class);   
        job.setReducerClass(Reduce.class);  
        //Mapper���������
        job.setOutputKeyClass(IntWritable.class);  
        job.setOutputValueClass(IntWritable.class);  
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));  
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));  
        System.exit(job.waitForCompletion(true) ? 0 : 1);  

    }

}
