package org.example.batch.reader;

import com.baomidou.dynamic.datasource.annotation.DS;
import lombok.extern.slf4j.Slf4j;
import org.example.mapper.CashVoucherMapper;
import org.example.mapper.ForexRateMapper;
import org.example.mapper.TransactionTypesMapper;
import org.example.pojo.ForexRatePO;
import org.example.pojo.TransactionTypesPO;
import org.example.pojo.dtos.CashVoucherWithRequestDTO;
import org.springframework.batch.item.ItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.math.RoundingMode;
import java.util.List;

/**
 * 优化的CashVoucher读取器
 * 支持分区读取，每个分区独立处理指定范围的数据
 */
@Slf4j
@Component
public class OptimizedCashVoucherReader implements ItemReader<CashVoucherWithRequestDTO> {

    private static final int FETCH_SIZE = 5000; // 每次从数据库获取5000条

    @Autowired
    private CashVoucherMapper cashVoucherMapper;
    
    @Autowired
    private TransactionTypesMapper transactionTypesMapper;
    
    @Autowired
    private ForexRateMapper forexRateMapper;

    private Long startIndex;
    private Long endIndex;
    private Integer partitionNumber;
    private Long currentOffset;
    private List<CashVoucherWithRequestDTO> currentBatch;
    private int currentBatchIndex;
    private long processedCount;

    public OptimizedCashVoucherReader() {
        // 默认构造函数，用于Spring代理
    }

    public OptimizedCashVoucherReader(Long startIndex, Long endIndex, Integer partitionNumber) {
        this.startIndex = startIndex;
        this.endIndex = endIndex;
        this.partitionNumber = partitionNumber;
        this.currentOffset = startIndex;
        this.currentBatchIndex = 0;
        this.processedCount = 0;
        log.info("初始化分区 {} 的Reader，范围: {} - {}", partitionNumber, startIndex, endIndex);
    }

    @Override
    @DS("master")
    public CashVoucherWithRequestDTO read() throws Exception {
        // 如果当前批次已读完，加载下一批
        if (currentBatch == null || currentBatchIndex >= currentBatch.size()) {
            if (currentOffset >= endIndex) {
                log.info("分区 {} 完成，共处理 {} 条记录", partitionNumber, processedCount);
                return null; // 该分区已完成
            }
            loadNextBatch();
        }

        // 如果加载后仍然没有数据，返回null
        if (currentBatch == null || currentBatch.isEmpty()) {
            return null;
        }

        // 返回当前批次的下一条记录
        CashVoucherWithRequestDTO dto = currentBatch.get(currentBatchIndex++);
        processedCount++;
        
        // 数据增强处理
        enrichData(dto);
        
        if (processedCount % 10000 == 0) {
            log.info("分区 {} 已处理 {} 条记录", partitionNumber, processedCount);
        }
        
        return dto;
    }

    /**
     * 从数据库加载下一批数据
     */
    @DS("master")
    private void loadNextBatch() {
        long remainingRecords = endIndex - currentOffset;
        int fetchSize = (int) Math.min(FETCH_SIZE, remainingRecords);
        
        if (fetchSize <= 0) {
            currentBatch = null;
            return;
        }

        try {
            currentBatch = cashVoucherMapper.selectCashVoucherWithRequest(
                currentOffset.intValue(), 
                fetchSize
            );
            currentOffset += fetchSize;
            currentBatchIndex = 0;
            
            log.debug("分区 {} 加载了 {} 条记录，当前偏移: {}", 
                partitionNumber, 
                currentBatch != null ? currentBatch.size() : 0, 
                currentOffset
            );
        } catch (Exception e) {
            log.error("分区 {} 加载数据失败，偏移: {}", partitionNumber, currentOffset, e);
            throw new RuntimeException("加载数据失败", e);
        }
    }

    /**
     * 数据增强 - 添加额外的计算字段
     */
    private void enrichData(CashVoucherWithRequestDTO dto) {
        try {
            // 设置交易类型
            TransactionTypesPO transactionTypesPO = transactionTypesMapper.selectTxnTypeCode(dto.getTxnType());
            if (transactionTypesPO != null) {
                dto.setTxnTypIdValue(transactionTypesPO.getSignIndicator().equals("C") ? 1L : 6L);
                dto.setTxnTypActnCdeValue(transactionTypesPO.getSignIndicator().equals("C") ? "IN" : "OUT");
            }
            
            // 设置汇率和基础货币金额
            if (dto.getConfirmationDate() != null && dto.getCcy() != null) {
                String dateStr = String.valueOf(dto.getConfirmationDate()).split("T")[0];
                ForexRatePO forexRatePO = forexRateMapper.selectRateByDate(dto.getCcy(), dateStr);
                if (forexRatePO != null && dto.getAmount() != null) {
                    dto.setBaseCcyEquAmtValue(
                        dto.getAmount()
                            .multiply(forexRatePO.getXRate())
                            .setScale(8, RoundingMode.HALF_UP)
                    );
                }
            }
        } catch (Exception e) {
            log.warn("分区 {} 数据增强失败: {}", partitionNumber, e.getMessage());
        }
    }
}
