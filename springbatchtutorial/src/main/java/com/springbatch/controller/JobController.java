package com.springbatch.controller;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/api/jobs")
public class JobController {
    @Autowired
    JobLauncher jobLauncher;
    @Autowired
    JobOperator jobOperator;
    @Qualifier("firstJob")
    @Autowired
    Job firstJob;
    @Qualifier("secondJob")
    @Autowired
    Job secondJob;
    @GetMapping("/start/{jobName}")
    public String startJob(@PathVariable String jobName) throws JobInstanceAlreadyCompleteException, JobExecutionAlreadyRunningException, JobParametersInvalidException, JobRestartException {
        Map<String, JobParameter> params = new HashMap<>();
        params.put("currentTime", new JobParameter(System.currentTimeMillis()));
        JobParameters jobParameters = new JobParameters(params);

        if(jobName.equals("First Job")){
            jobLauncher.run(firstJob, jobParameters);
        } else if (jobName.equals("Second Job")) {
            jobLauncher.run(secondJob, jobParameters);
        }
        return "Job Started...";
    }

    @GetMapping("/stop/{executionId}")
    public String stopJob(@PathVariable long jobExecutionId){
        try{
            jobOperator.stop(jobExecutionId);
        }catch (Exception e){
            System.out.println("Exception While Stoping The Job");
        }
        return "Job Stopped...";
    }
}
