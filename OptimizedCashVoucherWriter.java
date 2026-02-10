package org.example.batch.writer;

import com.baomidou.dynamic.datasource.annotation.DS;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.mapper.McAcFundRecMapper;
import org.example.mapper.McAcFundTxnRecMapper;
import org.example.mapper.McFundTpReltnMapper;
import org.example.pojo.McAcFundRecPO;
import org.example.pojo.McAcFundTxnRecPO;
import org.example.pojo.McFundTpReltnPO;
import org.example.pojo.dtos.CashVoucherResultDTO;
import org.example.utils.DataBaseOperationUtils;
import org.springframework.batch.item.ItemWriter;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 优化的CashVoucher写入器
 * 使用更大的批次和优化的事务管理
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class OptimizedCashVoucherWriter implements ItemWriter<CashVoucherResultDTO> {

    private static final int BATCH_SIZE = 2000; // 增大批量插入大小
    private final AtomicLong totalWritten = new AtomicLong(0);

    private final McAcFundRecMapper mcAcFundRecMapper;
    private final McAcFundTxnRecMapper mcAcFundTxnRecMapper;
    private final McFundTpReltnMapper mcFundTpReltnMapper;

    @Override
    @DS("oracle")
    @Transactional(rollbackFor = Exception.class, propagation = Propagation.REQUIRED)
    public void write(List<? extends CashVoucherResultDTO> items) throws Exception {
        if (items == null || items.isEmpty()) {
            return;
        }

        long startTime = System.currentTimeMillis();
        
        List<McAcFundRecPO> mcAcFundRecPORecord = new ArrayList<>(items.size());
        List<McAcFundTxnRecPO> mcAcFundTxnRecPORecord = new ArrayList<>(items.size());
        List<McFundTpReltnPO> mcFundTpReltnPORecord = new ArrayList<>(items.size());

        // 数据分组
        for (CashVoucherResultDTO dto : items) {
            if (dto.getMcAcFundRecRecord() != null) {
                mcAcFundRecPORecord.add(dto.getMcAcFundRecRecord());
            }
            if (dto.getMcAcFundTxnRecRecord() != null) {
                mcAcFundTxnRecPORecord.add(dto.getMcAcFundTxnRecRecord());
            }
            if (dto.getMcFundTpReltnPRecord() != null) {
                mcFundTpReltnPORecord.add(dto.getMcFundTpReltnPRecord());
            }
        }

        // 批量插入 - 按顺序插入以保持数据一致性
        try {
            if (!mcAcFundTxnRecPORecord.isEmpty()) {
                batchInsertOptimized(mcAcFundTxnRecPORecord, mcAcFundTxnRecMapper::batchInsert, "McAcFundTxnRec");
            }
            
            if (!mcAcFundRecPORecord.isEmpty()) {
                batchInsertOptimized(mcAcFundRecPORecord, mcAcFundRecMapper::batchInsert, "McAcFundRec");
            }
            
            if (!mcFundTpReltnPORecord.isEmpty()) {
                batchInsertOptimized(mcFundTpReltnPORecord, mcFundTpReltnMapper::batchInsert, "McFundTpReltn");
            }

            long written = totalWritten.addAndGet(items.size());
            long duration = System.currentTimeMillis() - startTime;
            
            if (written % 50000 == 0) {
                log.info("已写入 {} 条记录，本批次耗时: {} ms，平均速度: {} 条/秒", 
                    written, duration, items.size() * 1000 / Math.max(duration, 1));
            }
            
        } catch (Exception e) {
            log.error("批量写入失败，数据量: {}", items.size(), e);
            throw e;
        }
    }

    /**
     * 优化的批量插入方法
     */
    private <T> void batchInsertOptimized(List<T> records, 
                                          java.util.function.Consumer<List<T>> insertFunction,
                                          String tableName) {
        if (records.isEmpty()) {
            return;
        }

        int totalSize = records.size();
        int batchCount = (int) Math.ceil((double) totalSize / BATCH_SIZE);
        
        for (int i = 0; i < batchCount; i++) {
            int fromIndex = i * BATCH_SIZE;
            int toIndex = Math.min((i + 1) * BATCH_SIZE, totalSize);
            List<T> batch = records.subList(fromIndex, toIndex);
            
            try {
                insertFunction.accept(batch);
                log.debug("表 {} 批次 {}/{} 插入成功，大小: {}", tableName, i + 1, batchCount, batch.size());
            } catch (Exception e) {
                log.error("表 {} 批次 {}/{} 插入失败", tableName, i + 1, batchCount, e);
                throw new RuntimeException("批量插入失败: " + tableName, e);
            }
        }
    }

    /**
     * 重置计数器
     */
    public void reset() {
        totalWritten.set(0);
    }
}
