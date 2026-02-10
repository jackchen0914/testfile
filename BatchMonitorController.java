package org.example.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.*;
import java.util.stream.Collectors;

/**
 * 批处理任务监控控制器
 * 提供任务执行状态查询功能
 */
@RestController
@RequestMapping("/migration/monitor")
@RequiredArgsConstructor
public class BatchMonitorController {

    private final JobExplorer jobExplorer;

    /**
     * 查询指定任务的最新执行状态
     * 
     * @param jobName 任务名称，如：optimizedCashVoucherMigrationJob
     * @return 任务执行详情
     */
    @GetMapping("/job/{jobName}/latest")
    public Map<String, Object> getLatestJobStatus(@PathVariable String jobName) {
        Map<String, Object> result = new HashMap<>();
        
        try {
            List<JobInstance> jobInstances = jobExplorer.findJobInstancesByJobName(jobName, 0, 1);
            
            if (jobInstances.isEmpty()) {
                result.put("status", "NOT_FOUND");
                result.put("message", "未找到任务: " + jobName);
                return result;
            }
            
            JobInstance jobInstance = jobInstances.get(0);
            List<JobExecution> jobExecutions = jobExplorer.getJobExecutions(jobInstance);
            
            if (jobExecutions.isEmpty()) {
                result.put("status", "NO_EXECUTION");
                result.put("message", "任务未执行");
                return result;
            }
            
            // 获取最新的执行记录
            JobExecution latestExecution = jobExecutions.stream()
                    .max(Comparator.comparing(JobExecution::getCreateTime))
                    .orElse(null);
            
            if (latestExecution != null) {
                result.put("jobName", jobName);
                result.put("jobInstanceId", jobInstance.getId());
                result.put("jobExecutionId", latestExecution.getId());
                result.put("status", latestExecution.getStatus().toString());
                result.put("exitStatus", latestExecution.getExitStatus().getExitCode());
                result.put("createTime", latestExecution.getCreateTime());
                result.put("startTime", latestExecution.getStartTime());
                result.put("endTime", latestExecution.getEndTime());
                
                // 计算执行时长
                if (latestExecution.getStartTime() != null) {
                    Date endTime = latestExecution.getEndTime() != null ? 
                        latestExecution.getEndTime() : new Date();
                    long duration = endTime.getTime() - latestExecution.getStartTime().getTime();
                    result.put("durationSeconds", duration / 1000);
                    result.put("durationMinutes", duration / 60000);
                }
                
                // 获取Step执行信息
                List<Map<String, Object>> stepExecutions = latestExecution.getStepExecutions().stream()
                        .map(stepExecution -> {
                            Map<String, Object> stepInfo = new HashMap<>();
                            stepInfo.put("stepName", stepExecution.getStepName());
                            stepInfo.put("status", stepExecution.getStatus().toString());
                            stepInfo.put("readCount", stepExecution.getReadCount());
                            stepInfo.put("writeCount", stepExecution.getWriteCount());
                            stepInfo.put("commitCount", stepExecution.getCommitCount());
                            stepInfo.put("rollbackCount", stepExecution.getRollbackCount());
                            stepInfo.put("readSkipCount", stepExecution.getReadSkipCount());
                            stepInfo.put("writeSkipCount", stepExecution.getWriteSkipCount());
                            stepInfo.put("processSkipCount", stepExecution.getProcessSkipCount());
                            stepInfo.put("filterCount", stepExecution.getFilterCount());
                            
                            if (stepExecution.getStartTime() != null) {
                                Date stepEndTime = stepExecution.getEndTime() != null ? 
                                    stepExecution.getEndTime() : new Date();
                                long stepDuration = stepEndTime.getTime() - stepExecution.getStartTime().getTime();
                                stepInfo.put("durationSeconds", stepDuration / 1000);
                            }
                            
                            return stepInfo;
                        })
                        .collect(Collectors.toList());
                
                result.put("stepExecutions", stepExecutions);
                
                // 计算总处理记录数
                long totalReadCount = latestExecution.getStepExecutions().stream()
                        .mapToLong(se -> se.getReadCount())
                        .sum();
                long totalWriteCount = latestExecution.getStepExecutions().stream()
                        .mapToLong(se -> se.getWriteCount())
                        .sum();
                
                result.put("totalReadCount", totalReadCount);
                result.put("totalWriteCount", totalWriteCount);
                
                // 计算处理速度
                if (latestExecution.getStartTime() != null && totalWriteCount > 0) {
                    Date endTime = latestExecution.getEndTime() != null ? 
                        latestExecution.getEndTime() : new Date();
                    long duration = endTime.getTime() - latestExecution.getStartTime().getTime();
                    if (duration > 0) {
                        double recordsPerSecond = (double) totalWriteCount / (duration / 1000.0);
                        result.put("recordsPerSecond", Math.round(recordsPerSecond * 100.0) / 100.0);
                    }
                }
            }
            
        } catch (Exception e) {
            result.put("status", "ERROR");
            result.put("message", "查询失败: " + e.getMessage());
        }
        
        return result;
    }

    /**
     * 查询所有任务的执行历史
     * 
     * @return 任务列表
     */
    @GetMapping("/jobs")
    public Map<String, Object> getAllJobs() {
        Map<String, Object> result = new HashMap<>();
        
        try {
            Set<String> jobNames = jobExplorer.getJobNames();
            List<Map<String, Object>> jobs = new ArrayList<>();
            
            for (String jobName : jobNames) {
                List<JobInstance> instances = jobExplorer.findJobInstancesByJobName(jobName, 0, 5);
                
                for (JobInstance instance : instances) {
                    List<JobExecution> executions = jobExplorer.getJobExecutions(instance);
                    
                    for (JobExecution execution : executions) {
                        Map<String, Object> jobInfo = new HashMap<>();
                        jobInfo.put("jobName", jobName);
                        jobInfo.put("jobInstanceId", instance.getId());
                        jobInfo.put("jobExecutionId", execution.getId());
                        jobInfo.put("status", execution.getStatus().toString());
                        jobInfo.put("createTime", execution.getCreateTime());
                        jobInfo.put("startTime", execution.getStartTime());
                        jobInfo.put("endTime", execution.getEndTime());
                        
                        jobs.add(jobInfo);
                    }
                }
            }
            
            // 按创建时间倒序排序
            jobs.sort((a, b) -> {
                Date dateA = (Date) a.get("createTime");
                Date dateB = (Date) b.get("createTime");
                return dateB.compareTo(dateA);
            });
            
            result.put("totalJobs", jobs.size());
            result.put("jobs", jobs);
            
        } catch (Exception e) {
            result.put("status", "ERROR");
            result.put("message", "查询失败: " + e.getMessage());
        }
        
        return result;
    }

    /**
     * 查询正在运行的任务
     * 
     * @return 运行中的任务列表
     */
    @GetMapping("/jobs/running")
    public Map<String, Object> getRunningJobs() {
        Map<String, Object> result = new HashMap<>();
        
        try {
            Set<String> jobNames = jobExplorer.getJobNames();
            List<Map<String, Object>> runningJobs = new ArrayList<>();
            
            for (String jobName : jobNames) {
                Set<JobExecution> runningExecutions = jobExplorer.findRunningJobExecutions(jobName);
                
                for (JobExecution execution : runningExecutions) {
                    Map<String, Object> jobInfo = new HashMap<>();
                    jobInfo.put("jobName", jobName);
                    jobInfo.put("jobExecutionId", execution.getId());
                    jobInfo.put("status", execution.getStatus().toString());
                    jobInfo.put("startTime", execution.getStartTime());
                    
                    // 计算已运行时长
                    if (execution.getStartTime() != null) {
                        long duration = new Date().getTime() - execution.getStartTime().getTime();
                        jobInfo.put("runningDurationSeconds", duration / 1000);
                        jobInfo.put("runningDurationMinutes", duration / 60000);
                    }
                    
                    // 获取进度信息
                    long totalWriteCount = execution.getStepExecutions().stream()
                            .mapToLong(se -> se.getWriteCount())
                            .sum();
                    jobInfo.put("processedRecords", totalWriteCount);
                    
                    runningJobs.add(jobInfo);
                }
            }
            
            result.put("runningJobsCount", runningJobs.size());
            result.put("runningJobs", runningJobs);
            
        } catch (Exception e) {
            result.put("status", "ERROR");
            result.put("message", "查询失败: " + e.getMessage());
        }
        
        return result;
    }
}
