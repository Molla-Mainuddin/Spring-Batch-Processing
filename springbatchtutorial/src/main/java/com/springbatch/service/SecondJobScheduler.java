package com.springbatch.service;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

@Service
public class SecondJobScheduler {
    @Autowired
    JobLauncher jobLauncher;
    @Qualifier("firstJob")
    @Autowired
    Job firstJob;
    @Qualifier("secondJob")
    @Autowired
    Job secondJob;
    @Scheduled(cron = "0 0/5 * 1/1 * ?")
    public void secondJobStarter(){
        Map<String, JobParameter> params = new HashMap<>();
        params.put("currentTime", new JobParameter(System.currentTimeMillis()));
        JobParameters jobParameters = new JobParameters(params);
        try{
            JobExecution jobExecution = jobLauncher.run(secondJob, jobParameters);
            System.out.println("Job Execution Id :"+jobExecution.getId());
        }catch(Exception e){
            System.out.println("Exception While Starting Job");
        }
    }
}
