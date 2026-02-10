package org.example.service;

import lombok.extern.slf4j.Slf4j;
import org.example.batch.reader.*;
import org.springframework.batch.core.*;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.Date;

@Slf4j
@Service
public class BatchJobService{

    @Autowired
    private JobLauncher jobLauncher;

    @Autowired
    @Qualifier("portfolioFeeMigrationJob")
    private Job portfolioFeeMigrationJob;

    @Autowired
    private PortfolioFeeDailyReader portfolioFeeDailyReader;

    public String startPortfolioFeeMigration(){
        try {
            portfolioFeeDailyReader.reset();
            JobParameters jobParameters = new JobParametersBuilder()
                    .addLong("timestamp",System.currentTimeMillis())
                    .toJobParameters();
            jobLauncher.run(portfolioFeeMigrationJob, jobParameters);
            return "successfully";
        } catch (Exception e) {
            log.error("error",e);
            return "error: " + e.getMessage();
        }
    }

    //=========== CA Rights Split Consolidation ============
    @Autowired
    @Qualifier("caRSCFeeMigrationJob")
    private Job caRSCFeeMigrationJob;

    @Autowired
    private CaRSCReader caRSCReader;

    public String startCaRSCMigration(){
        try {
            caRSCReader.reset();
            JobParameters jobParameters = new JobParametersBuilder()
                    .addLong("timestamp",System.currentTimeMillis())
                    .toJobParameters();
            jobLauncher.run(caRSCFeeMigrationJob, jobParameters);
            return "successfully";
        } catch (Exception e) {
            log.error("error",e);
            return "error: " + e.getMessage();
        }
    }

    //=========== InstrumentVoucher ============
    @Autowired
    @Qualifier("instrumentVoucherMigrationJob")
    private Job instrumentVoucherMigrationJob;

    @Autowired
    private InstrumentVoucherReader instrumentVoucherReader;

    public String startInstrumentVoucherMigration(){
        try {
            instrumentVoucherReader.reset();
            JobParameters jobParameters = new JobParametersBuilder()
                    .addLong("timestamp",System.currentTimeMillis())
                    .toJobParameters();
            jobLauncher.run(instrumentVoucherMigrationJob, jobParameters);
            return "successfully";
        } catch (Exception e) {
            log.error("error",e);
            return "error: " + e.getMessage();
        }
    }

    //=========== ClntPriceCap ============
    @Autowired
    @Qualifier("clntPriceCapMigrationJob")
    private Job clntPriceCapMigrationJob;

    @Autowired
    private ClntPriceCapReader clntPriceCapReader;

    public String startClntPriceCapMigration(){
        try {
            clntPriceCapReader.reset();
            JobParameters jobParameters = new JobParametersBuilder()
                    .addLong("timestamp",System.currentTimeMillis())
                    .toJobParameters();
            jobLauncher.run(clntPriceCapMigrationJob, jobParameters);
            return "successfully";
        } catch (Exception e) {
            log.error("error",e);
            return "error: " + e.getMessage();
        }
    }

    //=========== CashTransferAll ============
    @Autowired
    @Qualifier("cashTransferAllMigrationJob")
    private Job cashTransferAllMigrationJob;

    @Autowired
    private CashTransferAllReader cashTransferAllReader;

    public String startCashTransferAllMigration(){
        try {
            cashTransferAllReader.reset();
            JobParameters jobParameters = new JobParametersBuilder()
                    .addLong("timestamp",System.currentTimeMillis())
                    .toJobParameters();
            jobLauncher.run(cashTransferAllMigrationJob, jobParameters);
            return "successfully";
        } catch (Exception e) {
            log.error("error",e);
            return "error: " + e.getMessage();
        }
    }

    //=========== Brokerage ============
    @Autowired
    @Qualifier("brokerageWithRageMigrationJob")
    private Job brokerageWithRageMigrationJob;

    @Autowired
    private BrokerageWithRageReader brokerageWithRageReader;

    public String startBrokerageWithRageMigration(){
        try {
            brokerageWithRageReader.reset();
            JobParameters jobParameters = new JobParametersBuilder()
                    .addLong("timestamp",System.currentTimeMillis())
                    .toJobParameters();
            jobLauncher.run(brokerageWithRageMigrationJob, jobParameters);
            return "successfully";
        } catch (Exception e) {
            log.error("error",e);
            return "error: " + e.getMessage();
        }
    }

    //=========== InterestDaily ============
    @Autowired
    @Qualifier("interestDailyMigrationJob")
    private Job interestDailyMigrationJob;

    @Autowired
    private InterestDailyReader interestDailyReader;

    public String startInterestDailyMigration(){
        try {
            interestDailyReader.reset();
            JobParameters jobParameters = new JobParametersBuilder()
                    .addLong("timestamp",System.currentTimeMillis())
                    .toJobParameters();
            jobLauncher.run(interestDailyMigrationJob, jobParameters);
            return "successfully";
        } catch (Exception e) {
            log.error("error",e);
            return "error: " + e.getMessage();
        }
    }

    //=========== CashVoucher ============
    @Autowired
    @Qualifier("cashVoucherMigrationJob")
    private Job cashVoucherMigrationJob;

    @Autowired
    private CashVoucherReader cashVoucherReader;

    public String startCashVoucherMigration(){
        try {
            cashVoucherReader.reset();
            JobParameters jobParameters = new JobParametersBuilder()
                    .addLong("timestamp",System.currentTimeMillis())
                    .toJobParameters();
            jobLauncher.run(cashVoucherMigrationJob, jobParameters);
            return "successfully";
        } catch (Exception e) {
            log.error("error",e);
            return "error: " + e.getMessage();
        }
    }

    //=========== Optimized CashVoucher (3000万数据优化版本) ============
    @Autowired
    @Qualifier("optimizedCashVoucherMigrationJob")
    private Job optimizedCashVoucherMigrationJob;

    @Autowired
    @Qualifier("asyncJobLauncher")
    private JobLauncher asyncJobLauncher;

    /**
     * 启动优化的CashVoucher迁移任务
     * 采用分区+多线程策略，适合处理3000万级别的大数据量
     * 
     * @return 任务执行结果
     */
    public String startOptimizedCashVoucherMigration(){
        try {
            long startTime = System.currentTimeMillis();
            log.info("开始执行优化的CashVoucher迁移任务...");
            
            JobParameters jobParameters = new JobParametersBuilder()
                    .addLong("timestamp", System.currentTimeMillis())
                    .addString("jobType", "optimized")
                    .toJobParameters();
            
            JobExecution jobExecution = jobLauncher.run(optimizedCashVoucherMigrationJob, jobParameters);
            
            long duration = System.currentTimeMillis() - startTime;
            String status = jobExecution.getStatus().toString();
            
            log.info("优化的CashVoucher迁移任务完成，状态: {}，耗时: {} 秒", 
                status, duration / 1000);
            
            return String.format("任务执行%s，耗时: %d 秒，状态: %s", 
                jobExecution.getStatus().isUnsuccessful() ? "失败" : "成功",
                duration / 1000,
                status);
        } catch (Exception e) {
            log.error("优化的CashVoucher迁移任务执行失败", e);
            return "error: " + e.getMessage();
        }
    }

    /**
     * 异步启动优化的CashVoucher迁移任务
     * 立即返回，任务在后台执行，避免HTTP超时
     * 
     * @return 任务启动结果
     */
    public String startOptimizedCashVoucherMigrationAsync(){
        try {
            log.info("异步启动优化的CashVoucher迁移任务...");
            
            JobParameters jobParameters = new JobParametersBuilder()
                    .addLong("timestamp", System.currentTimeMillis())
                    .addString("jobType", "optimized-async")
                    .toJobParameters();
            
            // 使用异步JobLauncher，立即返回
            asyncJobLauncher.run(optimizedCashVoucherMigrationJob, jobParameters);
            
            log.info("优化的CashVoucher迁移任务已在后台启动");
            
            return "任务已成功启动，正在后台执行。请通过日志或监控接口查看进度。";
        } catch (Exception e) {
            log.error("启动优化的CashVoucher迁移任务失败", e);
            return "error: " + e.getMessage();
        }
    }

    //=========== HoldCash ============
    @Autowired
    @Qualifier("holdCashMigrationJob")
    private Job holdCashMigrationJob;

    @Autowired
    private HoldCashReader holdCashReader;

    public String startHoldCashMigration(){
        try {
            holdCashReader.reset();
            JobParameters jobParameters = new JobParametersBuilder()
                    .addLong("timestamp",System.currentTimeMillis())
                    .toJobParameters();
            jobLauncher.run(holdCashMigrationJob, jobParameters);
            return "successfully";
        } catch (Exception e) {
            log.error("error",e);
            return "error: " + e.getMessage();
        }
    }

    //=========== HoldInstrument ============
    @Autowired
    @Qualifier("holdInstrumentMigrationJob")
    private Job holdInstrumentMigrationJob;

    @Autowired
    private HoldInstrumentReader holdInstrumentReader;

    public String startHoldInstrumentMigration(){
        try {
            holdInstrumentReader.reset();
            JobParameters jobParameters = new JobParametersBuilder()
                    .addLong("timestamp",System.currentTimeMillis())
                    .toJobParameters();
            jobLauncher.run(holdInstrumentMigrationJob, jobParameters);
            return "successfully";
        } catch (Exception e) {
            log.error("error",e);
            return "error: " + e.getMessage();
        }
    }

    //=========== Full Migration ============
    @Autowired
    @Qualifier("fullMigrationJob")
    private Job fullMigrationJob;

    public String runFullMigration() {
        try {
            portfolioFeeDailyReader.reset();
            caRSCReader.reset();
            JobParameters jobParameters = new JobParametersBuilder()
                    .addLong("timestamp",System.currentTimeMillis())
                    .toJobParameters();
            jobLauncher.run(fullMigrationJob, jobParameters);
            return "all successfully";
        } catch (Exception e) {
            log.error("error",e);
            return "all error: " + e.getMessage();
        }
    }

}
