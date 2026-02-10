package org.example.controller;

import lombok.RequiredArgsConstructor;
import org.example.service.BatchJobService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/migration")
@RequiredArgsConstructor
public class BatchJobController {

    private final BatchJobService batchJobService;

    @GetMapping("/portfolioFee")
    public String startPortfolioFeeMigration(){
        return batchJobService.startPortfolioFeeMigration();
    }

    @GetMapping("/caRSC")
    public String startCaRSCMigration(){
        return batchJobService.startCaRSCMigration();
    }

    @GetMapping("/instrumentVoucher")
    public String startInstrumentVoucherMigration(){
        return batchJobService.startInstrumentVoucherMigration();
    }

    @GetMapping("/clntPriceCap")
    public String startClntPriceCapMigration(){
        return batchJobService.startClntPriceCapMigration();
    }

    @GetMapping("/cashTransferAll")
    public String startCashTransferAllMigration(){
        return batchJobService.startCashTransferAllMigration();
    }

    @GetMapping("/brokerageWithRage")
    public String startBrokerageWithRageMigration(){
        return batchJobService.startBrokerageWithRageMigration();
    }

    @GetMapping("/interestDaily")
    public String startInterestDailyMigration(){
        return batchJobService.startInterestDailyMigration();
    }

    @GetMapping("/cashVoucher")
    public String startCashVoucherMigration(){
        return batchJobService.startCashVoucherMigration();
    }

    /**
     * 优化的CashVoucher迁移接口 - 适合3000万级别大数据量
     * 采用分区+多线程策略，显著提升处理速度
     */
    @GetMapping("/cashVoucher/optimized")
    public String startOptimizedCashVoucherMigration(){
        return batchJobService.startOptimizedCashVoucherMigration();
    }

    /**
     * 异步优化的CashVoucher迁移接口 - 推荐使用
     * 任务在后台执行，立即返回，避免HTTP超时
     * 适合3000万级别大数据量的长时间运行任务
     */
    @GetMapping("/cashVoucher/optimized/async")
    public String startOptimizedCashVoucherMigrationAsync(){
        return batchJobService.startOptimizedCashVoucherMigrationAsync();
    }

    @GetMapping("/holdCash")
    public String startHoldCashMigration(){
        return batchJobService.startHoldCashMigration();
    }

    @GetMapping("/holdInstrument")
    public String startHoldInstrumentMigration(){
        return batchJobService.startHoldInstrumentMigration();
    }

    @GetMapping("/full")
    public String migrateAll(){
        return batchJobService.runFullMigration();
    }
}
