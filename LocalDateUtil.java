package com.clfimb.common.utils;

import com.clfimb.common.enums.TermUnitEnum;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.time.Month;
import java.time.Year;
import java.time.YearMonth;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.TextStyle;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAdjusters;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * LocalDate 工具类
 * @author zhaod
 */
@SuppressWarnings("unused")
public class LocalDateUtil {

    /**
     * 获取日期的年份
     * @param date 日期
     * @return int
     */
    public static int getThisYear(LocalDate date){
        return date.getYear();
    }

    /**
     * 获取日期的月份
     * @param date 日期
     * @return int
     */
    public static int getThisMonth(LocalDate date){
        return date.getMonthValue();
    }

    /**
     * 获取日期的上个月年份
     * @param date 日期
     * @return int
     */
    public static int getLastMonthYear(LocalDate date){
        return date.minusMonths(1).getYear();
    }

    /**
     * 获取日期的上个月月份
     * @param date 日期
     * @return int
     */
    public static int getLastMonth(LocalDate date){
        return date.minusMonths(1).getMonthValue();
    }

    /**
     * 将 java.util.Date 转换为 java.time.LocalDate
     * @param date 日期对象 (如果为null则返回null)
     * @return LocalDate 对象
     */
    public static LocalDate dateToLocalDate(Date date) {
        if (date == null) {
            return null;
        }
        return date.toInstant()
                .atZone(ZoneId.systemDefault())
                .toLocalDate();
    }

    /**
     * 如果你需要Date对象（例如与旧代码兼容），可以提供一个静态方法来获取它
     * @param localDate 日期
     * @return Date
     */
    public static Date localDateToDate(LocalDate localDate) {
        return Date.from(localDate.atStartOfDay(java.time.ZoneId.systemDefault()).toInstant());
    }


    /**
     * @param localDate 日期
     * @return Date
     */
    public static String getChineseMonth(LocalDate localDate) {
        return localDate.getMonthValue() + "月";
    }

    /**
     * @param localDate 日期
     * @return Date
     */
    public static String getChineseLastMonth(LocalDate localDate) {
        return localDate.minusMonths(1).getMonthValue() + "月";
    }

    /**
     * @param localDate 日期
     * @return Date
     */
    public static String getChineseYear(LocalDate localDate) {
        return localDate.getYear() + "年";
    }


    /**
     * @param localDate 日期
     * @return Date
     */
    public static String getEnglishMonth(LocalDate localDate) {
        return localDate.getMonth().getDisplayName(TextStyle.FULL, Locale.ENGLISH);
    }

    /**
     * 如果你需要Date对象（例如与旧代码兼容），可以提供一个静态方法来获取它
     * @param localDate 日期
     * @return Date
     */
    public static String getEnglishLastMonth(LocalDate localDate) {
        return localDate.minusMonths(1).getMonth().getDisplayName(TextStyle.FULL, Locale.ENGLISH);
    }


    private static final DateTimeFormatter FORMATTER_SIMPLE = DateTimeFormatter.ofPattern("yyyyMMdd");

    public static String formatSimpleDate(LocalDate date) {
        return date.format(FORMATTER_SIMPLE);
    }



    public static String formatDate(LocalDate date, String formatPattern) {
        return Date8Util.toDateString(date, formatPattern);
    }

    /**
     * 计算从 quoteDate 到 quoteDate + depositTerm 月之间的天数，并返回 BigDecimal 类型的结果
     *
     * @param quoteDate 报价日期
     * @param depositTerm 存款期限（单位：月）
     * @return 间隔的天数，BigDecimal 类型
     */
    public static BigDecimal calculateDaysBetweenAsBigDecimal(LocalDate quoteDate, BigDecimal depositTerm) {
        // 将存款期限转为整数
        int months = depositTerm.intValue();

        // 计算结束日期
        LocalDate endDate = quoteDate.plusMonths(months);

        // 计算两个日期之间的天数，并转换为 BigDecimal 类型
        long days = ChronoUnit.DAYS.between(quoteDate, endDate);
        return BigDecimal.valueOf(days);
    }

    /**
     * 计算从 quoteDate 到 quoteDate + depositTerm 单位之间的天数，并返回 BigDecimal 类型的结果
     *
     * @param quoteDate    报价日期
     * @param depositTerm  存款期限（单位：月、年、季度等）
     * @param termUnitEnum 存款期限的单位（如 月、年、季度等）
     * @return 间隔的天数，BigDecimal 类型
     */
    public static BigDecimal calculateDaysBetweenAsBigDecimal(LocalDate quoteDate, BigDecimal depositTerm, TermUnitEnum termUnitEnum) {
        if (depositTerm == null){
            return null;
        }
        // 将存款期限转为整数
        int termValue = depositTerm.intValue();

        // 计算结束日期，根据不同单位处理
        LocalDate endDate = switch (termUnitEnum) {
            case MONTH ->
                // 如果单位是月，直接加上月份
                    quoteDate.plusMonths(termValue);
            case YEAR ->
                // 如果单位是年，转换为月份再加上
                    quoteDate.plusYears(termValue);
            case QUARTER ->
                // 如果单位是季度，按季度转换为月并加上
                    quoteDate.plusMonths(termValue * 3L);
            case WEEK ->
                // 如果单位是周，转换为天数加上
                    quoteDate.plusWeeks(termValue);
            case DAY ->
                // 如果单位是天，直接加上天数
                    quoteDate.plusDays(termValue);
        };

        // 计算两个日期之间的天数，并转换为 BigDecimal 类型
        long days = ChronoUnit.DAYS.between(quoteDate, endDate);
        return BigDecimal.valueOf(days);
    }


    /**
     * 计算从 quoteDate 到 quoteDate + depositTerm 天之间相隔的月数（四舍五入）
     *
     * @param quoteDate 报价日期
     * @param depositTerm 存款期限（单位：天）
     * @return 间隔的月数，BigDecimal 类型
     */
    public static BigDecimal calculateMonthsBetweenAsBigDecimal(LocalDate quoteDate, BigDecimal depositTerm) {
        // 将存款期限从天数转为整数
        int days = depositTerm.intValue();

        // 计算结束日期
        LocalDate endDate = quoteDate.plusDays(days);

        // 计算两个日期之间的完整月数
        long totalMonths = ChronoUnit.MONTHS.between(quoteDate, endDate);

        // 计算剩余天数
        LocalDate partialMonthStartDate = quoteDate.plusMonths(totalMonths);
        long remainingDays = ChronoUnit.DAYS.between(partialMonthStartDate, endDate);

        // 转换为月数（假定每月 30 天，四舍五入计算部分月）
        BigDecimal remainingMonths = BigDecimal.valueOf(remainingDays).divide(BigDecimal.valueOf(30), 2, RoundingMode.HALF_UP);

        // 总月数 = 完整月数 + 部分月数
        return BigDecimal.valueOf(totalMonths).add(remainingMonths).setScale(2, RoundingMode.HALF_UP).stripTrailingZeros();
    }

    // 将LocalDate转换为Date
    public static Date toDate(LocalDate businessDate) {
        return Date.from(businessDate.atStartOfDay(ZoneId.systemDefault()).toInstant());
    }

    public static LocalDate getLastDayOfLastYear(){
        // 获取当前年份并减1
        int lastYear = Year.now().getValue() - 1;
        // 创建去年12月31日的日期
        return LocalDate.of(lastYear, 12, 31);
    }

    public static LocalDate getLastDayOfLastMonth(LocalDate businessDate){
        return  businessDate.minusMonths(1)
                .with(TemporalAdjusters.lastDayOfMonth());
    }



    public static LocalDate getFirstDayOfThisMonth(){
        LocalDate now = LocalDate.now();
        // 将日期调整为当前月的第一天
        return now.withDayOfMonth(1);
    }

    /**
     * 获取上月最后一个工作日（排除周六日）
     */
    public static LocalDate getLastWorkingDayOfPreviousMonth() {
        // 获取上月最后一天
        LocalDate lastDay = LocalDate.now()
                .minusMonths(1)
                .with(TemporalAdjusters.lastDayOfMonth());

        // 调整到最后一个工作日
        return adjustToWorkingDay(lastDay);
    }

    public static LocalDate getLastWorkingDayOfPreviousMonth(LocalDate businessDate) {
        // 获取上月最后一天
        LocalDate lastDay = businessDate
                .minusMonths(1)
                .with(TemporalAdjusters.lastDayOfMonth());

        // 调整到最后一个工作日
        return adjustToWorkingDay(lastDay);
    }

    private static LocalDate adjustToWorkingDay(LocalDate date) {
        return switch (date.getDayOfWeek()) {
            case SATURDAY -> date.minusDays(1);
            case SUNDAY -> date.minusDays(2);
            default -> date;
        };
    }

    public static LocalDate getFirstDayOfThisYear(){
        LocalDate now = LocalDate.now();
        now = now.withDayOfMonth(1); // 将日期调整为当前月的第一天
        return now.withMonth(1); // 将日期调整为当前月的第一天
    }

    public static LocalDate getFirstDayOfLastYear(){
        // 获取当前年份并减1
        int lastYear = Year.now().getValue() - 1;
        // 创建去年12月31日的日期
        return LocalDate.of(lastYear, 1, 1);
    }

    /**
     * 生成两个日期之间的日期列表（包含起止日期）
     * @param startDate 开始日期（非空）
     * @param endDate 结束日期（非空且不小于开始日期）
     * @return 日期列表
     * @throws IllegalArgumentException 如果参数无效
     */
    public static List<LocalDate> getDatesBetween(LocalDate startDate, LocalDate endDate) {
        Objects.requireNonNull(startDate, "开始日期不能为null");
        Objects.requireNonNull(endDate, "结束日期不能为null");

        if (endDate.isBefore(startDate)) {
            throw new IllegalArgumentException("结束日期不能早于开始日期");
        }

        return Stream.iterate(startDate, date -> date.plusDays(1))
                .limit(startDate.until(endDate).getDays() + 1)
                .collect(Collectors.toList());
    }

    /**
     *
     * @param businessDate businessDate
     * @return
     */
    public static List<LocalDate> getMonthEndsUpToDate(LocalDate businessDate) {
        List<LocalDate> monthEnds = new ArrayList<>();
        int currentYear = businessDate.getYear();

        for (int month = 1; month <= businessDate.getMonthValue(); month++) {
            LocalDate endOfMonth = YearMonth.of(currentYear, month).atEndOfMonth();
            monthEnds.add(endOfMonth);
        }

        return monthEnds;
    }
}
