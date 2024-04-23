package org.example;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class CustomOutputCommitter extends OutputCommitter {

  private FileOutputCommitter fileOutputCommitter;
  Map<Integer,String> demo;

  public CustomOutputCommitter(Path outputPath, TaskAttemptContext context) throws IOException {
    System.out.println("TASKID : " + context.getTaskAttemptID());
    fileOutputCommitter = new FileOutputCommitter(outputPath, context);
    demo = new HashMap<>();
    demo.put(demo.size(), "CustomOutputCommitter");
  }

  @Override
  public void setupJob(JobContext jobContext) throws IOException {
    fileOutputCommitter.setupJob(jobContext);
    demo.put(demo.size(), "setupJob");
  }

  @Override
  public void setupTask(TaskAttemptContext taskContext) throws IOException {
    fileOutputCommitter.setupTask(taskContext);
    demo.put(demo.size(), "setupTask");
  }

  @Override
  public boolean needsTaskCommit(TaskAttemptContext taskContext) throws IOException {
    demo.put(demo.size(), "needsTaskCommit");
    return fileOutputCommitter.needsTaskCommit(taskContext);
  }

  @Override
  public void commitTask(TaskAttemptContext taskContext) throws IOException {
    demo.put(demo.size(), "commitTask");
    fileOutputCommitter.commitTask(taskContext);
  }

  @Override
  public void commitJob(JobContext jobContext) throws IOException {
    demo.put(demo.size(), "commitJob");
    fileOutputCommitter.commitJob(jobContext);
  }

  @Override
  public void abortTask(TaskAttemptContext taskContext) throws IOException {
    demo.put(demo.size(), "abortTask");
    fileOutputCommitter.abortTask(taskContext);
  }
}
