package org.example;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class CustomFileOutputFormat extends FileOutputFormat<LongWritable, Text> {

  @Override
  public RecordWriter<LongWritable, Text> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
    // delegate to TextOutputFormat's getRecordWriter
    return new TextOutputFormat<LongWritable, Text>().getRecordWriter(job);
  }

  @Override
  public Path getDefaultWorkFile(TaskAttemptContext context, String extension) throws IOException {
    // delegate to FileOutputFormat's getDefaultWorkFile
    return super.getDefaultWorkFile(context, extension);
  }

  @Override
  public synchronized OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException {
    System.out.println("CustomFileOutputFormat.getOutputCommitter");
    return new CustomOutputCommitter(getOutputPath(context), context);
  }
}