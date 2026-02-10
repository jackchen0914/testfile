package org.example.config;

import org.example.batch.processor.CashVoucherProcessor;
import org.example.batch.reader.OptimizedCashVoucherReader;
import org.example.batch.writer.CashVoucherWriter;
import org.example.pojo.dtos.CashVoucherResultDTO;
import org.example.pojo.dtos.CashVoucherWithRequestDTO;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.partition.support.MultiResourcePartitioner;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.HashMap;
import java.util.Map;

/**
 * 优化的批处理配置 - 针对3000万数据的高性能处理
 * 采用分区+多线程策略
 */
@Configuration
public class OptimizedBatchConfig {

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Autowired
    private CashVoucherProcessor cashVoucherProcessor;

    @Autowired
    private CashVoucherWriter cashVoucherWriter;

    /**
     * 多线程任务执行器
     * 核心线程数：10
     * 最大线程数：20
     * 队列容量：100
     */
    @Bean(name = "batchTaskExecutor")
    public TaskExecutor batchTaskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(10);
        executor.setMaxPoolSize(20);
        executor.setQueueCapacity(100);
        executor.setThreadNamePrefix("batch-thread-");
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.setAwaitTerminationSeconds(60);
        executor.initialize();
        return executor;
    }

    /**
     * 数据分区器 - 将3000万数据分成多个分区
     * 每个分区处理100万数据
     */
    @Bean
    public Partitioner cashVoucherPartitioner() {
        return gridSize -> {
            Map<String, org.springframework.batch.item.ExecutionContext> partitions = new HashMap<>();
            
            // 假设总数据量为30,000,000，每个分区处理1,000,000条
            long totalRecords = 30000000L;
            long partitionSize = 1000000L;
            int partitionCount = (int) Math.ceil((double) totalRecords / partitionSize);
            
            for (int i = 0; i < partitionCount; i++) {
                org.springframework.batch.item.ExecutionContext context = new org.springframework.batch.item.ExecutionContext();
                long startIndex = i * partitionSize;
                long endIndex = Math.min((i + 1) * partitionSize, totalRecords);
                
                context.putLong("startIndex", startIndex);
                context.putLong("endIndex", endIndex);
                context.putLong("partitionSize", partitionSize);
                context.putInt("partitionNumber", i);
                
                partitions.put("partition" + i, context);
            }
            
            return partitions;
        };
    }

    /**
     * 分区感知的Reader - 每个分区独立读取数据
     */
    @Bean
    @StepScope
    public ItemReader<CashVoucherWithRequestDTO> partitionedCashVoucherReader(
            @Value("#{stepExecutionContext['startIndex']}") Long startIndex,
            @Value("#{stepExecutionContext['endIndex']}") Long endIndex,
            @Value("#{stepExecutionContext['partitionNumber']}") Integer partitionNumber) {
        return new OptimizedCashVoucherReader(startIndex, endIndex, partitionNumber);
    }

    /**
     * Worker Step - 实际处理数据的步骤
     * chunk size设置为5000以提高吞吐量
     */
    @Bean
    public Step cashVoucherWorkerStep() {
        return stepBuilderFactory.get("cashVoucherWorkerStep")
                .<CashVoucherWithRequestDTO, CashVoucherResultDTO>chunk(5000)
                .reader(partitionedCashVoucherReader(null, null, null))
                .processor(cashVoucherProcessor)
                .writer(cashVoucherWriter)
                .faultTolerant()
                .skipLimit(100)
                .skip(Exception.class)
                .build();
    }

    /**
     * Master Step - 分区管理步骤
     * 使用10个线程并行处理分区
     */
    @Bean
    public Step cashVoucherPartitionStep() {
        return stepBuilderFactory.get("cashVoucherPartitionStep")
                .partitioner("cashVoucherWorkerStep", cashVoucherPartitioner())
                .step(cashVoucherWorkerStep())
                .gridSize(10) // 同时运行10个分区
                .taskExecutor(batchTaskExecutor())
                .build();
    }

    /**
     * 优化的CashVoucher迁移Job
     */
    @Bean(name = "optimizedCashVoucherMigrationJob")
    public Job optimizedCashVoucherMigrationJob() {
        return jobBuilderFactory.get("optimizedCashVoucherMigrationJob")
                .incrementer(new RunIdIncrementer())
                .start(cashVoucherPartitionStep())
                .build();
    }
}
