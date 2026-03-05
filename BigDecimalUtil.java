package com.clfimb.common.utils;

import org.apache.commons.lang3.StringUtils;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.ParsePosition;
import java.util.*;

/**
 * @author zhaod
 */
public class BigDecimalUtil {


    /**
     * </p>
     * 字符串转BigDecimal，忽略千分号
     * </p>
     * @Params:
     * @param str 文件流
     * @Author: ZOUYIWEI399
     * @Since: 2023-05-22 15:35
     */
    public static BigDecimal string2BigDecimal(String str) {
        if (StringUtils.isEmpty(str)) {
            return BigDecimal.ZERO;
        }
        str.replace("(","");
        str.replace(")","");

        try {
            // 直接使用BigDecimal构造方法，自动处理正负号
            return new BigDecimal(str.trim());
        } catch (NumberFormatException e) {
            // 若直接构造失败（如含千分位逗号），尝试DecimalFormat解析
            DecimalFormat format = new DecimalFormat();
            format.setParseBigDecimal(true);
            ParsePosition position = new ParsePosition(0);
            BigDecimal result = (BigDecimal) format.parse(str.trim(), position);
            // 检查是否完整解析且非空
            if (result == null || position.getIndex() != str.trim().length()) {
                return BigDecimal.ZERO; // 解析失败返回0或抛异常
            }
            return result;
        }
    }


    /**
     * </p>
     * 计算平均值
     * </p>
     * @Params:
     * @param bigDecimalList bigDecimalList
     * @Author: ZOUYIWEI399
     * @Since: 2023-05-22 15:35
     */
    public static BigDecimal avg(List<BigDecimal> bigDecimalList) {
        List<BigDecimal> noNullList = bigDecimalList.stream().filter(Objects::nonNull).toList();
        return noNullList.stream().reduce(BigDecimal.ZERO, BigDecimal :: add).divide(BigDecimal.valueOf(noNullList.size()), 2, RoundingMode.HALF_UP);
    }


    /**
     * BigDecimal 转 String
     * @param value 值
     * @return String
     */
    public static String getString(BigDecimal value){
        if(value == null){
            return "";
        }
        BigDecimal bigDecimal = value.stripTrailingZeros();
        return bigDecimal.toPlainString();
    }


    /**
     * BigDecimal 转 String, 四舍五入保留【scale】位小数
     * @param value 值
     * @return String
     */
    public static String getString(BigDecimal value, Integer scale){
        if(value == null){
            return "";
        }
        BigDecimal bigDecimal = value.stripTrailingZeros().setScale(scale, RoundingMode.HALF_UP);
        return bigDecimal.toPlainString();
    }

    /**
     * BigDecimal 转 String, 后面增加【%】
     * @param value 值
     * @return String
     */
    public static String getStringWithCode(BigDecimal value){
        String percentString = getString(value);
        if(percentString.isEmpty()){
            return percentString;
        }
        return percentString + "%";
    }


    /**
     * BigDecimal 转 String, 四舍五入保留【scale】位小数, 后面增加【%】
     * @param value 值
     * @return String
     */
    public static String getStringWithCode(BigDecimal value, Integer scale){
        String percentString = getString(value, scale);
        if(percentString.isEmpty()){
            return percentString;
        }
        return percentString + "%";
    }

    /**
     * BigDecimal 转百分比（*100）String ,并保留两位小数直接舍弃
     * @param value 值
     * @return String
     */
    public static String getPercentString(BigDecimal value){
        if(value == null){
            return "";
        }
        BigDecimal bigDecimal = value.multiply(BigDecimal.valueOf(100)).stripTrailingZeros();
        return bigDecimal.toPlainString();
    }

    /**
     * BigDecimal 转百分比（*100）String ,四舍五入保留【scale】位小数
     * @param value 值
     * @return String
     */
    public static String getPercentString(BigDecimal value, Integer scale){
        if(value == null){
            return "";
        }
        BigDecimal bigDecimal = value.multiply(BigDecimal.valueOf(100)).setScale(scale, RoundingMode.HALF_UP);
        return bigDecimal.toPlainString();
    }

    /**
     * BigDecimal 转百分比（*100）String, 后面增加【%】
     * @param value 值
     * @return String
     */
    public static String getPercentStringWithCode(BigDecimal value){
        String percentString = getPercentString(value);
        if(percentString.isEmpty()){
            return percentString;
        }
        if("0".equals(percentString)){
            return "-";
        }
        return percentString + "%";
    }

    /**
     * BigDecimal 转百分比（*100）String ,四舍五入保留【scale】位小数, 后面增加【%】
     * @param value 值
     * @return String
     */
    public static String getPercentStringWithCode(BigDecimal value, Integer scale){
        String percentString = getPercentString(value, scale);
        if(percentString.isEmpty()){
            return percentString;
        }
        return percentString + "%";
    }

    public static String formatToMillion(BigDecimal value) {
        if (value == null) {
            return "0M";
        }
        BigDecimal million = new BigDecimal(1_000_000);
        BigDecimal result = value.divide(million);
        // 格式化为千分位并保留两位小数
        DecimalFormat df = new DecimalFormat("#,##0.##");
        return df.format(result) + "M";
    }

    /**
     * 将 BigDecimal 金额转换为字符串，保留指定小数位数，并添加千分位逗号分隔符
     *
     * @param amount 金额
     * @param scale  保留的小数位数
     * @return 格式化后的金额字符串
     */
    public static String getAmountStringWithScale(BigDecimal amount, int scale) {
        if (amount == null) {
            return "0.00";
        }

        // 设置小数位数
        amount = amount.setScale(scale, RoundingMode.HALF_UP);

        // 创建 DecimalFormat 实例，设置千分位分隔符
        DecimalFormat decimalFormat = (DecimalFormat) NumberFormat.getInstance(Locale.US);
        // 动态生成小数位数
        decimalFormat.applyPattern("#,##0." + "0".repeat(scale));

        return decimalFormat.format(amount);
    }

    public static boolean isNumeric4(String str) {
        return str != null && str.chars().allMatch(Character::isDigit);
    }


    /**
     * 计算多个 BigDecimal 的平均值
     *
     * @param scale        精度（小数位数）
     * @param roundingMode 舍入模式
     * @param values       可变参数，支持传入多个 BigDecimal
     * @return 平均值，如果无有效值则返回 BigDecimal.ZERO
     */
    public static BigDecimal average(int scale, RoundingMode roundingMode, BigDecimal... values) {
        if (values == null || values.length == 0) {
            return BigDecimal.ZERO;
        }

        // 过滤掉 null 值
        BigDecimal[] validValues = Arrays.stream(values)
                .filter(Objects::nonNull)
                .toArray(BigDecimal[]::new);

        if (validValues.length == 0) {
            return BigDecimal.ZERO;
        }

        // 计算总和
        BigDecimal sum = Arrays.stream(validValues)
                .reduce(BigDecimal.ZERO, BigDecimal::add);

        // 计算平均值
        return sum.divide(new BigDecimal(validValues.length), scale, roundingMode);
    }

    /**
     * 计算多个 BigDecimal 的平均值，默认精度为 4 位小数，舍入模式为 HALF_UP
     *
     * @param values 可变参数，支持传入多个 BigDecimal
     * @return 平均值，如果无有效值则返回 BigDecimal.ZERO
     */
    public static BigDecimal average(BigDecimal... values) {
        return average(4, RoundingMode.HALF_UP, values);
    }


    public static String formatAmountWithFullScale(BigDecimal value) {
        return NumberFormat.getNumberInstance(Locale.US)
                .format(value.stripTrailingZeros());
    }

    /**
     * 比较大小，如果是null，默认为0
     *
     * @param value1
     * @param value2
     * @return 平均值，如果无有效值则返回 BigDecimal.ZERO
     */
    public static int compareTo(BigDecimal value1, BigDecimal value2) {
        value1 = Objects.isNull(value1) ? BigDecimal.ZERO : value1;
        value2 = Objects.isNull(value2) ? BigDecimal.ZERO : value2;
        return value1.compareTo(value2);
    }

    /**
     * 将字符串解析为BigDecimal对象
     * @param value 要解析的字符串，可能包含逗号作为千位分隔符
     * @return 解析后的BigDecimal对象，如果输入为null则返回null
     */
    public static BigDecimal parseBigDecimal(String value) {
        return Optional.ofNullable(value)
                .map(v -> new BigDecimal(v.replaceAll("[,()（）]", "")))
                .orElse(null);
    }

    /**
     * BigDecimal 相加，判断是否为null
     * @param value1 值1
     * @param value2 值2
     * @return String
     */
    public static BigDecimal add(BigDecimal value1, BigDecimal value2) {
        if (Objects.isNull(value1) && Objects.isNull(value2)) {
            return null;
        } else if (Objects.isNull(value1)) {
            return value2;
        } else if (Objects.isNull(value2)) {
            return value1;
        } else {
            return value1.add(value2);
        }
    }
}
