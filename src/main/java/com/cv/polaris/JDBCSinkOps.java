package com.cv.polaris;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Row;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;


class JDBCSinkOps {
    private static Logger log = Logger.getLogger(JDBCSinkOps.class);

    public static PreparedStatement addParamsToStatement(PreparedStatement statement, Row value) throws SQLException {
        log.info("JDBC sink ops: adding params to statement: " + new Timestamp(System.currentTimeMillis()));
        statement.setString(1, value.getAs("PAYMENT_EVENT_KEY"));
        statement.setString(2, value.getAs("PAYMENT_EVENT_KEY"));
        statement.setString(3, value.getAs("PAYMENT_EVENT_KEY"));
        statement.setString(4, value.getAs("PAYMENT_EVENT_ID"));
        if (StringUtils.isBlank(value.getAs(("TIME_CREATED_MS")))) {
            statement.setString(5, "".toString());
        } else {
            statement.setBigDecimal(5, BigDecimal.valueOf(Double.parseDouble(value.getAs("TIME_CREATED_MS"))));
        }
        statement.setString(6, value.getAs("PAYMENT_KEY"));
        statement.setString(7, value.getAs("PAYMENT_ID"));
        statement.setString(8, value.getAs("PAYMENT_ID_ENCRYPT"));
        statement.setString(9, value.getAs("PAIRED_PAYMENT_KEY"));
        statement.setString(10, value.getAs("FINANCIAL_ACTIVITY_KEY"));
        statement.setString(11, value.getAs("ACTIVITY_PARTICIPANT_KEY"));
        statement.setString(12, value.getAs("PARTY_KEY"));
        statement.setString(13, value.getAs("ACCOUNT_KEY"));
        statement.setString(14, value.getAs("PRICING_CATEGORY_GROUP_CODE"));
        statement.setString(15, value.getAs("PRICING_CATEGORY_CODE"));
        statement.setString(16, value.getAs("COUNTRY_CODE"));
        statement.setString(17, value.getAs("PAYMENT_TYPE_CODE"));
        statement.setString(18, value.getAs("PAYMENT_SUBTYPE_CODE"));
        statement.setString(19, value.getAs("PAYMENT_REASON_CODE"));
        if (StringUtils.isBlank(value.getAs(("PEER_REFERENCE_NUMBER")))) {
            statement.setString(20, "".toString());
        } else {
            statement.setLong(20, Long.parseLong(value.getAs("PEER_REFERENCE_NUMBER")));
        }
        statement.setString(21, value.getAs("MEASUREMENT_TYPE_CODE"));
        statement.setString(22, value.getAs("CURRENCY_CODE"));
        statement.setString(23, value.getAs("DEBIT_CREDIT_CODE"));
        if (StringUtils.isBlank(value.getAs(("PAYMENT_AMOUNT")))) {
            statement.setString(24, "".toString());
        } else {
            statement.setBigDecimal(24, BigDecimal.valueOf(Double.parseDouble(value.getAs("PAYMENT_AMOUNT"))));
        }
        if (StringUtils.isBlank(value.getAs(("PAYMENT_USD_AMOUNT")))) {
            statement.setString(25, "".toString());
        } else {
            statement.setBigDecimal(25, BigDecimal.valueOf(Double.parseDouble(value.getAs("PAYMENT_USD_AMOUNT"))));
        }
        if (StringUtils.isBlank(value.getAs(("ORIGINAL_AUTHORIZATION_NUMBER")))) {
            statement.setString(26, "".toString());
        } else {
            statement.setLong(26, Long.parseLong(value.getAs("ORIGINAL_AUTHORIZATION_NUMBER")));
        }
        if (StringUtils.isBlank(value.getAs(("AUTHORIZATION_NUMBER")))) {
            statement.setString(27, "".toString());
        } else {
            statement.setLong(27, Long.parseLong(value.getAs("AUTHORIZATION_NUMBER")));
        }
        statement.setString(28, value.getAs("AUTHORIZATION_STATUS_CODE"));
        if (StringUtils.isBlank(value.getAs(("AUTHORIZED_PAYMENT_AMOUNT")))) {
            statement.setString(29, "".toString());
        } else {
            statement.setBigDecimal(29,
                    BigDecimal.valueOf(Double.parseDouble(value.getAs("AUTHORIZED_PAYMENT_AMOUNT"))));
        }
        if (StringUtils.isBlank(value.getAs(("TIME_HONORED_MS")))) {
            statement.setString(30, "".toString());
        } else {
            statement.setLong(30, Long.parseLong(value.getAs("TIME_HONORED_MS")));
        }
        if (StringUtils.isBlank(value.getAs(("TIME_EXPIRED_MS")))) {
            statement.setString(31, "".toString());
        } else {
            statement.setLong(31, Long.parseLong(value.getAs("TIME_EXPIRED_MS")));
        }
        statement.setString(32, value.getAs("PAYMENT_STATUS_CODE"));
        if (StringUtils.isBlank(value.getAs(("TIME_PAYMENT_STATUS_MS")))) {
            statement.setString(33, "".toString());
        } else {
            statement.setBigDecimal(33, BigDecimal.valueOf(Double.parseDouble(value.getAs("TIME_PAYMENT_STATUS_MS"))));
        }
        if (StringUtils.isBlank(value.getAs(("TIME_COMPLETED_MS")))) {
            statement.setString(34, "".toString());
        } else {
            statement.setBigDecimal(34, BigDecimal.valueOf(Double.parseDouble(value.getAs("TIME_COMPLETED_MS"))));
        }
        if (StringUtils.isBlank(value.getAs(("APR")))) {
            statement.setString(35, "".toString());
        } else {
            statement.setLong(35, Long.parseLong(value.getAs("APR")));
        }
        statement.setString(36, value.getAs("PROMOTION_FUNDING_CODE"));
        statement.setString(37, value.getAs("COUNTER_PARTICIPANT_KEY"));
        statement.setString(38, value.getAs("COUNTER_PARTY_KEY"));
        statement.setString(39, value.getAs("COUNTER_EMAIL_ADDRESS"));
        statement.setString(40, value.getAs("COUNTER_CREDENTIAL_TYPE_CODE"));
        statement.setString(41, value.getAs("COUNTER_CREDENTIAL_ID"));
        statement.setString(42, value.getAs("COUNTER_CREDENTIAL_VALUE"));
        statement.setString(43, value.getAs("COUNTER_MOST_RECENT_IP_ADDRESS"));
        statement.setString(44, value.getAs("REFERENCE_VALUE"));
        if (StringUtils.isBlank(value.getAs(("FROM_MONEY_AMOUNT")))) {
            statement.setString(45, "".toString());
        } else {
            statement.setBigDecimal(45, BigDecimal.valueOf(Double.parseDouble(value.getAs("FROM_MONEY_AMOUNT"))));
        }
        statement.setString(46, value.getAs("FROM_CURRENCY_CODE"));
        if (StringUtils.isBlank(value.getAs(("TO_MONEY_AMOUNT")))) {
            statement.setString(47, "".toString());
        } else {
            statement.setBigDecimal(47, BigDecimal.valueOf(Double.parseDouble(value.getAs("TO_MONEY_AMOUNT"))));
        }
        statement.setString(48, value.getAs("TO_CURRENCY_CODE"));
        statement.setString(49, value.getAs("IS_BALANCE_IMPACTING"));
        statement.setString(50, value.getAs("IS_COMMERCIAL_ENTITY"));
        statement.setString(51, value.getAs("IS_LEDGER_IMPACTING"));
        statement.setString(52, value.getAs("PAYMENT_FLAGS").toString());
        statement.setString(53, value.getAs("PARTICIPANT_PAYMENT_NOTE"));
        statement.setString(54, value.getAs("PARTICIPANT_PAYMENT_MEMO"));
        statement.setString(55, value.getAs("PARENT_PAYMENT_KEY"));
        statement.setString(56, value.getAs("PRIMARY_GIVEN_NAME"));
        statement.setString(57, value.getAs("COUNTER_PRIMARY_GIVEN_NAME"));
        statement.setString(58, value.getAs("PAYMENT_EVENT_TYPE_CODE"));
        statement.setString(59, value.getAs("AFFILIATE_PAYMENT_ID"));
        statement.setString(60, value.getAs("AFFILIATE_PAYMENT_ID_ENCRYPT"));
        statement.setString(61, value.getAs("PARTY_ROLE_CODE"));
        statement.setString(62, value.getAs("COUNTER_IS_COMMERCIAL_ENTITY"));
        statement.setString(63, value.getAs("ACTIVITY_ACCOUNT_KEY"));
        statement.setString(64, value.getAs("ACCOUNT_UUID"));
        statement.setString(65, value.getAs("ACCOUNT_CLASS_CODE"));
        if (StringUtils.isBlank(value.getAs(("ACCOUNT_PRODUCT_KEY")))) {
            statement.setString(66, "".toString());
        } else {
            statement.setLong(66, Long.parseLong(value.getAs("ACCOUNT_PRODUCT_KEY")));
        }
        statement.setString(67, value.getAs("ACCOUNT_BALANCE_KEY"));
        statement.setString(68, value.getAs("TENDER_TYPE_CODE"));
        statement.setString(69, value.getAs("VALID_COUNTRY_CODE"));
        if (StringUtils.isBlank(value.getAs(("PRODUCT_KEY")))) {
            statement.setString(70, "".toString());
        } else {
            statement.setLong(70, Long.parseLong(value.getAs("PRODUCT_KEY")));
        }
        if (StringUtils.isBlank(value.getAs(("PRODUCT_FEATURE_KEY")))) {
            statement.setString(71, "".toString());
        } else {
            statement.setLong(71, Long.parseLong(value.getAs("PRODUCT_FEATURE_KEY")));
        }
        if (StringUtils.isBlank(value.getAs(("CHANNEL_KEY")))) {
            statement.setString(72, "".toString());
        } else {
            statement.setLong(72, Long.parseLong(value.getAs("CHANNEL_KEY")));
        }
        statement.setString(73, value.getAs("CAMPAIGN_KEY"));
        statement.setString(74, value.getAs("PAYMENT_REFERENCE_NUMBER"));
        statement.setString(75, value.getAs("REVERSAL_STATUS_CODE"));
        if (StringUtils.isBlank(value.getAs(("PAYMENT_FN_AMOUNT")))) {
            statement.setString(76, "".toString());
        } else {
            statement.setBigDecimal(76, BigDecimal.valueOf(Double.parseDouble(value.getAs("PAYMENT_FN_AMOUNT"))));
        }
        if (StringUtils.isBlank(value.getAs(("AUTHORIZED_PAYMENT_USD_AMT")))) {
            statement.setString(77, "".toString());
        } else {
            statement.setBigDecimal(77,
                    BigDecimal.valueOf(Double.parseDouble(value.getAs("AUTHORIZED_PAYMENT_USD_AMT"))));
        }
        if (StringUtils.isBlank(value.getAs(("AUTHORIZED_PAYMENTN_FN_AMT")))) {
            statement.setString(78, "".toString());
        } else {
            statement.setBigDecimal(78,
                    BigDecimal.valueOf(Double.parseDouble(value.getAs("AUTHORIZED_PAYMENTN_FN_AMT"))));
        }
        if (StringUtils.isBlank(value.getAs(("CAPTURED_COUNT")))) {
            statement.setString(79, "".toString());
        } else {
            statement.setLong(79, Long.parseLong(value.getAs("CAPTURED_COUNT")));
        }
        if (StringUtils.isBlank(value.getAs(("CAPTURED_AMOUNT")))) {
            statement.setString(80, "".toString());
        } else {
            statement.setBigDecimal(80, BigDecimal.valueOf(Double.parseDouble(value.getAs("CAPTURED_AMOUNT"))));
        }
        if (StringUtils.isBlank(value.getAs(("CAPTURED_USD_AMOUNT")))) {
            statement.setString(81, "".toString());
        } else {
            statement.setBigDecimal(81, BigDecimal.valueOf(Double.parseDouble(value.getAs("CAPTURED_USD_AMOUNT"))));
        }
        if (StringUtils.isBlank(value.getAs(("CAPTURED_FN_AMOUNT")))) {
            statement.setString(82, "".toString());
        } else {
            statement.setBigDecimal(82, BigDecimal.valueOf(Double.parseDouble(value.getAs("CAPTURED_FN_AMOUNT"))));
        }
        statement.setString(83, value.getAs("FEE_CURRENCY_CODE2"));
        statement.setString(84, value.getAs("FEE_DEBIT_CREDIT_CODE"));
        if (StringUtils.isBlank(value.getAs(("PAYMENT_FEE")))) {
            statement.setString(85, "".toString());
        } else {
            statement.setBigDecimal(85, BigDecimal.valueOf(Double.parseDouble(value.getAs("PAYMENT_FEE"))));
        }
        if (StringUtils.isBlank(value.getAs(("PAYMENT_USD_FEE")))) {
            statement.setString(86, "".toString());
        } else {
            statement.setBigDecimal(86, BigDecimal.valueOf(Double.parseDouble(value.getAs("PAYMENT_USD_FEE"))));
        }
        if (StringUtils.isBlank(value.getAs(("PAYMENT_FN_FEE")))) {
            statement.setString(87, "".toString());
        } else {
            statement.setBigDecimal(87, BigDecimal.valueOf(Double.parseDouble(value.getAs("PAYMENT_FN_FEE"))));
        }
        statement.setString(88, value.getAs("PAYMENT_ADJUSTMENTS"));
        statement.setString(89, value.getAs("PAYMENT_USD_ADJUSTMENTS"));
        statement.setString(90, value.getAs("PAYMENT_FN_ADJUSTMENTS"));
        statement.setString(91, value.getAs("FUNCTIONAL_CURRENCY_CODE"));
        statement.setString(92, value.getAs("PAYMENT_STATUS_REASON_CODE"));
        statement.setString(93, value.getAs("FEE_IS_LIMITED_TO_GROSS"));
        statement.setString(94, value.getAs("HAD_EXCHANGE_FEES_WAIVED"));
        statement.setString(95, value.getAs("HAS_CROSS_BORDER_FEE"));
        if (StringUtils.isBlank(value.getAs(("TOTAL_ACCOUNT_BALANCE")))) {
            statement.setString(96, "".toString());
        } else {
            statement.setBigDecimal(96, BigDecimal.valueOf(Double.parseDouble(value.getAs("TOTAL_ACCOUNT_BALANCE"))));
        }
        if (StringUtils.isBlank(value.getAs(("TOTAL_ACCOUNT_USD_BALANCE")))) {
            statement.setString(97, "".toString());
        } else {
            statement.setBigDecimal(97,
                    BigDecimal.valueOf(Double.parseDouble(value.getAs("TOTAL_ACCOUNT_USD_BALANCE"))));
        }
        if (StringUtils.isBlank(value.getAs(("TOTAL_ACCOUNT_FN_BALANCE")))) {
            statement.setString(98, "".toString());
        } else {
            statement
                    .setBigDecimal(98, BigDecimal.valueOf(Double.parseDouble(value.getAs("TOTAL_ACCOUNT_FN_BALANCE"))));
        }
        if (StringUtils.isBlank(value.getAs(("AVAILABLE_ACCOUNT_BALANCE")))) {
            statement.setString(99, "".toString());
        } else {
            statement.setBigDecimal(99,
                    BigDecimal.valueOf(Double.parseDouble(value.getAs("AVAILABLE_ACCOUNT_BALANCE"))));
        }
        if (StringUtils.isBlank(value.getAs(("AVAILABLE_ACCOUNT_USD_BALANCE")))) {
            statement.setString(100, "".toString());
        } else {
            statement.setBigDecimal(100,
                    BigDecimal.valueOf(Double.parseDouble(value.getAs("AVAILABLE_ACCOUNT_USD_BALANCE"))));
        }
        if (StringUtils.isBlank(value.getAs(("AVAILABLE_ACCOUNT_FN_BALANCE")))) {
            statement.setString(101, "".toString());
        } else {
            statement.setBigDecimal(101,
                    BigDecimal.valueOf(Double.parseDouble(value.getAs("AVAILABLE_ACCOUNT_FN_BALANCE"))));
        }
        statement.setString(102, value.getAs("FUNDING_CHECK_TYPE_CODE"));
        statement.setString(103, value.getAs("FEE_DATA_VERSION_NUMBER"));
        statement.setString(104, value.getAs("HAS_CROSS_CURRENCY_FEE"));
        if (StringUtils.isBlank(value.getAs(("TERM_COUNT")))) {
            statement.setString(105, "".toString());
        } else {
            statement.setLong(105, Long.parseLong(value.getAs("TERM_COUNT")));
        }
        statement.setString(106, value.getAs("TERM_UNIT_OF_MEASURE_CODE"));
        statement.setString(107, value.getAs("FINANCING_TYPE_CODE"));
        statement.setString(108, value.getAs("COUNTER_PARTY_NAME"));
        statement.setString(109, value.getAs("COUNTER_ALTERNATE_PARTY_NAME"));
        statement.setString(110, value.getAs("COUNTER_PARTY_ROLE_CODE"));
        statement.setString(111, value.getAs("HAS_SIG_INTERNATIONAL_FEE"));
        statement.setString(112, value.getAs("COUNTER_ACTIVITY_ACCOUNT_KEY"));
        statement.setString(113, value.getAs("COUNTER_ACCOUNT_KEY"));
        statement.setString(114, value.getAs("COUNTER_ACCOUNT_UUID"));
        statement.setString(115, value.getAs("COUNTER_ACCOUNT_CLASS_CODE"));
        if (StringUtils.isBlank(value.getAs(("COUNTER_ACCOUNT_PRODUCT_KEY")))) {
            statement.setString(116, "".toString());
        } else {
            statement.setLong(116, Long.parseLong(value.getAs("COUNTER_ACCOUNT_PRODUCT_KEY")));
        }
        statement.setString(117, value.getAs("COUNTER_PRICING_GROUP_CODE"));
        statement.setString(118, value.getAs("COUNTER_PRICING_CATEGORY_CODE"));
        statement.setString(119, value.getAs("COUNTER_DOCUMENT_KEY"));
        statement.setString(120, value.getAs("COUNTER_DOCUMENT_TYPE_CODE"));
        statement.setString(121, value.getAs("COUNTER_PHONE_CARRIER_NAME"));
        statement.setString(122, value.getAs("COUNTER_PHONE_NUMBER"));
        statement.setString(123, value.getAs("COUNTER_CITY_NAME"));
        statement.setString(124, value.getAs("COUNTER_STATE_NAME"));
        statement.setString(125, value.getAs("COUNTER_POSTAL_AREA_CODE"));
        statement.setString(126, value.getAs("COUNTER_COUNTRY_CODE"));
        statement.setString(127, value.getAs("COUNTER_VALID_COUNTRY_CODE"));
        statement.setString(128, value.getAs("COUNTER_TIMEZONE_CODE"));
        statement.setString(129, value.getAs("COUNTER_LANGUAGE_CODE"));
        if (StringUtils.isBlank(value.getAs(("APPLICATION_SYSTEM_KEY")))) {
            statement.setString(130, "".toString());
        } else {
            statement.setLong(130, Long.parseLong(value.getAs("APPLICATION_SYSTEM_KEY")));
        }
        if (StringUtils.isBlank(value.getAs(("SERVER_KEY")))) {
            statement.setString(131, "".toString());
        } else {
            statement.setLong(131, Long.parseLong(value.getAs("SERVER_KEY")));
        }
        if (StringUtils.isBlank(value.getAs(("DATA_ELEMENT_KEY")))) {
            statement.setString(132, "".toString());
        } else {
            statement.setLong(132, Long.parseLong(value.getAs("DATA_ELEMENT_KEY")));
        }
        statement.setString(133, value.getAs("IS_ACTIVE"));
        statement.setString(134, value.getAs("ASSIGNING_PARTY_KEY"));
        statement.setString(135, value.getAs("RELATED_PAYMENT_KEY"));
        statement.setString(136, value.getAs("RELATED_PAYMENT_ROLE_CODE"));
        if (StringUtils.isBlank(value.getAs(("ADJUSTMENT_NUMBER")))) {
            statement.setString(137, "".toString());
        } else {
            statement.setLong(137, Long.parseLong(value.getAs("ADJUSTMENT_NUMBER")));
        }
        statement.setString(138, value.getAs("ADJUSTMENT_TYPE_CODE"));
        if (StringUtils.isBlank(value.getAs(("FIXED_AMOUNT")))) {
            statement.setString(139, "".toString());
        } else {
            statement.setBigDecimal(139, BigDecimal.valueOf(Double.parseDouble(value.getAs("FIXED_AMOUNT"))));
        }
        if (StringUtils.isBlank(value.getAs(("FIXED_USD_AMOUNT")))) {
            statement.setString(140, "".toString());
        } else {
            statement.setBigDecimal(140, BigDecimal.valueOf(Double.parseDouble(value.getAs("FIXED_USD_AMOUNT"))));
        }
        if (StringUtils.isBlank(value.getAs(("FIXED_FN_AMOUNT")))) {
            statement.setString(141, "".toString());
        } else {
            statement.setBigDecimal(141, BigDecimal.valueOf(Double.parseDouble(value.getAs("FIXED_FN_AMOUNT"))));
        }
        if (StringUtils.isBlank(value.getAs(("VARIABLE_RATE")))) {
            statement.setString(142, "".toString());
        } else {
            statement.setBigDecimal(142, BigDecimal.valueOf(Double.parseDouble(value.getAs("VARIABLE_RATE"))));
        }
        if (StringUtils.isBlank(value.getAs(("VARIABLE_AMOUNT")))) {
            statement.setString(143, "".toString());
        } else {
            statement.setBigDecimal(143, BigDecimal.valueOf(Double.parseDouble(value.getAs("VARIABLE_AMOUNT"))));
        }
        if (StringUtils.isBlank(value.getAs(("VARIABLE_USD_AMOUNT")))) {
            statement.setString(144, "".toString());
        } else {
            statement.setBigDecimal(144, BigDecimal.valueOf(Double.parseDouble(value.getAs("VARIABLE_USD_AMOUNT"))));
        }
        if (StringUtils.isBlank(value.getAs(("VARIABLE_FN_AMOUNT")))) {
            statement.setString(145, "".toString());
        } else {
            statement.setBigDecimal(145, BigDecimal.valueOf(Double.parseDouble(value.getAs("VARIABLE_FN_AMOUNT"))));
        }
        if (StringUtils.isBlank(value.getAs(("GROSS_AMOUNT")))) {
            statement.setString(146, "".toString());
        } else {
            statement.setBigDecimal(146, BigDecimal.valueOf(Double.parseDouble(value.getAs("GROSS_AMOUNT"))));
        }
        if (StringUtils.isBlank(value.getAs(("GROSS_USD_AMOUNT")))) {
            statement.setString(147, "".toString());
        } else {
            statement.setBigDecimal(147, BigDecimal.valueOf(Double.parseDouble(value.getAs("GROSS_USD_AMOUNT"))));
        }
        if (StringUtils.isBlank(value.getAs(("GROSS_FN_AMOUNT")))) {
            statement.setString(148, "".toString());
        } else {
            statement.setBigDecimal(148, BigDecimal.valueOf(Double.parseDouble(value.getAs("GROSS_FN_AMOUNT"))));
        }
        if (StringUtils.isBlank(value.getAs(("TAX_AMOUNT")))) {
            statement.setString(149, "".toString());
        } else {
            statement.setBigDecimal(149, BigDecimal.valueOf(Double.parseDouble(value.getAs("TAX_AMOUNT"))));
        }
        if (StringUtils.isBlank(value.getAs(("TAX_USD_AMOUNT")))) {
            statement.setString(150, "".toString());
        } else {
            statement.setBigDecimal(150, BigDecimal.valueOf(Double.parseDouble(value.getAs("TAX_USD_AMOUNT"))));
        }
        if (StringUtils.isBlank(value.getAs(("TAX_FN_AMOUNT")))) {
            statement.setString(151, "".toString());
        } else {
            statement.setBigDecimal(151, BigDecimal.valueOf(Double.parseDouble(value.getAs("TAX_FN_AMOUNT"))));
        }
        if (StringUtils.isBlank(value.getAs(("NET_AMOUNT")))) {
            statement.setString(152, "".toString());
        } else {
            statement.setBigDecimal(152, BigDecimal.valueOf(Double.parseDouble(value.getAs("NET_AMOUNT"))));
        }
        if (StringUtils.isBlank(value.getAs(("NET_USD_AMOUNT")))) {
            statement.setString(153, "".toString());
        } else {
            statement.setBigDecimal(153, BigDecimal.valueOf(Double.parseDouble(value.getAs("NET_USD_AMOUNT"))));
        }
        if (StringUtils.isBlank(value.getAs(("NET_FN_AMOUNT")))) {
            statement.setString(154, "".toString());
        } else {
            statement.setBigDecimal(154, BigDecimal.valueOf(Double.parseDouble(value.getAs("NET_FN_AMOUNT"))));
        }
        if (StringUtils.isBlank(value.getAs(("BUY_EXCHANGE_RATE")))) {
            statement.setString(155, "".toString());
        } else {
            statement.setBigDecimal(155, BigDecimal.valueOf(Double.parseDouble(value.getAs("BUY_EXCHANGE_RATE"))));
        }
        if (StringUtils.isBlank(value.getAs(("SELL_EXCHANGE_RATE")))) {
            statement.setString(156, "".toString());
        } else {
            statement.setBigDecimal(156, BigDecimal.valueOf(Double.parseDouble(value.getAs("SELL_EXCHANGE_RATE"))));
        }
        if (StringUtils.isBlank(value.getAs(("HANDOVER_EXCHANGE_RATE")))) {
            statement.setString(157, "".toString());
        } else {
            statement.setBigDecimal(157, BigDecimal.valueOf(Double.parseDouble(value.getAs("HANDOVER_EXCHANGE_RATE"))));
        }
        if (StringUtils.isBlank(value.getAs(("EXCHANGE_TAKE_RATE")))) {
            statement.setString(158, "".toString());
        } else {
            statement.setBigDecimal(158, BigDecimal.valueOf(Double.parseDouble(value.getAs("EXCHANGE_TAKE_RATE"))));
        }
        if (StringUtils.isBlank(value.getAs(("FROM_USD_AMOUNT")))) {
            statement.setString(159, "".toString());
        } else {
            statement.setBigDecimal(159, BigDecimal.valueOf(Double.parseDouble(value.getAs("FROM_USD_AMOUNT"))));
        }
        if (StringUtils.isBlank(value.getAs(("TO_USD_AMOUNT")))) {
            statement.setString(160, "".toString());
        } else {
            statement.setBigDecimal(160, BigDecimal.valueOf(Double.parseDouble(value.getAs("TO_USD_AMOUNT"))));
        }
        if (StringUtils.isBlank(value.getAs(("EXCHANGE_RATE_CUT_AMOUNT")))) {
            statement.setString(161, "".toString());
        } else {
            statement.setBigDecimal(161,
                    BigDecimal.valueOf(Double.parseDouble(value.getAs("EXCHANGE_RATE_CUT_AMOUNT"))));
        }
        if (StringUtils.isBlank(value.getAs(("EXCHANGE_RATE_CUT_USD_AMOUNT")))) {
            statement.setString(162, "".toString());
        } else {
            statement.setBigDecimal(162,
                    BigDecimal.valueOf(Double.parseDouble(value.getAs("EXCHANGE_RATE_CUT_USD_AMOUNT"))));
        }
        statement.setString(163, value.getAs("OFFER_PARTICIPANT_KEY"));
        statement.setString(164, value.getAs("OFFER_KEY"));
        if (StringUtils.isBlank(value.getAs(("REMAINING_OFFER_VALUE")))) {
            statement.setString(165, "".toString());
        } else {
            statement.setBigDecimal(165, BigDecimal.valueOf(Double.parseDouble(value.getAs("REMAINING_OFFER_VALUE"))));
        }
        statement.setString(166, value.getAs("BANK_TRANSACTION_TYPE_CODE"));
        if (StringUtils.isBlank(value.getAs(("SETTLEMENT_TRANCHE_NUMBER")))) {
            statement.setString(167, "".toString());
        } else {
            statement.setLong(167, Long.parseLong(value.getAs("SETTLEMENT_TRANCHE_NUMBER")));
        }
        if (StringUtils.isBlank(value.getAs(("RESPONSE_CUTOFF_TIME")))) {
            statement.setString(168, "".toString());
        } else {
            statement.setLong(168, Long.parseLong(value.getAs("RESPONSE_CUTOFF_TIME")));
        }
        if (StringUtils.isBlank(value.getAs(("BANK_ROUTING_KEY")))) {
            statement.setString(169, "".toString());
        } else {
            statement.setLong(169, Long.parseLong(value.getAs("BANK_ROUTING_KEY")));
        }
        statement.setString(170, value.getAs("BANK_NAME"));
        statement.setString(171, value.getAs("BANK_COUNTRY_CODE"));
        statement.setString(172, value.getAs("BANK_ACCOUNT_NAME"));
        statement.setString(173, value.getAs("BANK_ACCOUNT_UUID"));
        statement.setString(174, value.getAs("PAYMENT_ROUTING_NUMBER"));
        if (StringUtils.isBlank(value.getAs(("INTERNAL_TRANCHE_NUMBER")))) {
            statement.setString(175, "".toString());
        } else {
            statement.setLong(175, Long.parseLong(value.getAs("INTERNAL_TRANCHE_NUMBER")));
        }
        statement.setString(176, value.getAs("PROCESSOR_PARTY_KEY"));
        statement.setString(177, value.getAs("REMITTANCE_LABEL"));
        statement.setString(178, value.getAs("BANK_CLEARING_TYPE_CODE"));
        statement.setString(179, value.getAs("BANK_MANDATE_CODE"));
        statement.setString(180, value.getAs("BANK_MANDATE_SIGN_DATE"));
        statement.setString(181, value.getAs("IS_FORCED"));
        statement.setString(182, value.getAs("CLEARING_RESPONSE_CODE"));
        if (StringUtils.isBlank(value.getAs(("PUSH_TRANSACTION_KEY")))) {
            statement.setString(183, "".toString());
        } else {
            statement.setLong(183, Long.parseLong(value.getAs("PUSH_TRANSACTION_KEY")));
        }
        statement.setString(184, value.getAs("PUSH_TRANSACTION_IDENTIFIER"));
        if (StringUtils.isBlank(value.getAs(("TIME_PUSH_TRANSACTION_MS")))) {
            statement.setString(185, "".toString());
        } else {
            statement.setLong(185, Long.parseLong(value.getAs("TIME_PUSH_TRANSACTION_MS")));
        }
        statement.setString(186, value.getAs("EXTERNAL_PAYMENT_IDENTIFIER"));
        statement.setString(187, value.getAs("PROCESSOR_AGENT_NAME"));
        statement.setString(188, value.getAs("PUSH_TRANSFER_TYPE_CODE"));
        statement.setString(189, value.getAs("PUSH_TRANSFER_SUBTYPE_CODE"));
        statement.setString(190, value.getAs("PUSH_TRANSFER_STATUS_CODE"));
        statement.setString(191, value.getAs("CARD_DOMAIN_CODE"));
        statement.setString(192, value.getAs("CARD_PAYMENT_TYPE_CODE"));
        statement.setString(193, value.getAs("CARD_PAYMENT_STATUS_CODE"));
        if (StringUtils.isBlank(value.getAs(("SYSTEM_TRACE_AUDIT_NUMBER")))) {
            statement.setString(194, "".toString());
        } else {
            statement.setLong(194, Long.parseLong(value.getAs("SYSTEM_TRACE_AUDIT_NUMBER")));
        }
        if (StringUtils.isBlank(value.getAs(("CARD_NETWORK_ID")))) {
            statement.setString(195, "".toString());
        } else {
            statement.setLong(195, Long.parseLong(value.getAs("CARD_NETWORK_ID")));
        }
        statement.setString(196, value.getAs("PAYMENT_CARD_KEY"));
        statement.setString(197, value.getAs("PAYMENT_CARD_UUID"));
        statement.setString(198, value.getAs("CARDHOLDER_SURNAME"));
        statement.setString(199, value.getAs("CARDHOLDER_GIVEN_NAME"));
        if (StringUtils.isBlank(value.getAs(("EXPIRY_YEAR")))) {
            statement.setString(200, "".toString());
        } else {
            statement.setLong(200, Long.parseLong(value.getAs("EXPIRY_YEAR")));
        }
        if (StringUtils.isBlank(value.getAs(("EXPIRY_MONTH")))) {
            statement.setString(201, "".toString());
        } else {
            statement.setLong(201, Long.parseLong(value.getAs("EXPIRY_MONTH")));
        }
        statement.setString(202, value.getAs("CARD_ACCEPTOR_NAME"));
        statement.setString(203, value.getAs("CARD_ACCEPTOR_ID"));
        statement.setString(204, value.getAs("CARD_ACCEPTOR_TERMINAL_ID"));
        statement.setString(205, value.getAs("TERMINAL_TYPE_CODE"));
        statement.setString(206, value.getAs("AUTHORIZER_REFERENCE_NUMBER"));
        statement.setString(207, value.getAs("SECONDARY_REFERENCE_NUMBER"));
        statement.setString(208, value.getAs("PROCESSING_AGENT_PARTY_KEY"));
        if (StringUtils.isBlank(value.getAs(("PROCESSING_CODE")))) {
            statement.setString(209, "".toString());
        } else {
            statement.setLong(209, Long.parseLong(value.getAs("PROCESSING_CODE")));
        }
        statement.setString(210, value.getAs("ACQUIRER_BANK_PARTY_KEY"));
        statement.setString(211, value.getAs("ACQUIRER_AGENT_PARTY_KEY"));
        statement.setString(212, value.getAs("ACQUIRER_COUNTRY_CODE"));
        statement.setString(213, value.getAs("ACQUIRER_RESPONSE_CODE"));
        statement.setString(214, value.getAs("AUTHORIZER_BANK_PARTY_KEY"));
        statement.setString(215, value.getAs("AUTHORIZER_AGENT_PARTY_KEY"));
        statement.setString(216, value.getAs("AUTHORIZER_COUNTRY_CODE"));
        statement.setString(217, value.getAs("AUTHORIZATION_CODE"));
        statement.setString(218, value.getAs("AUTHORIZATION_CURRENCY_CODE"));
        statement.setString(219, value.getAs("AUTHORIZATION_ID"));
        if (StringUtils.isBlank(value.getAs(("AUTHORIZATION_ID_MAX_LENGTH")))) {
            statement.setString(220, "".toString());
        } else {
            statement.setLong(220, Long.parseLong(value.getAs("AUTHORIZATION_ID_MAX_LENGTH")));
        }
        statement.setString(221, value.getAs("ORIGINAL_RESPONSE_CODE"));
        statement.setString(222, value.getAs("AUTHORIZER_RESPONSE_CODE"));
        statement.setString(223, value.getAs("AUTHORIZER_REASON_CODE"));
        statement.setString(224, value.getAs("AUTHORIZER_ADVICE_CODE"));
        statement.setString(225, value.getAs("AUTHORIZER_MESSAGE"));
        statement.setString(226, value.getAs("AVS_RESULT_CODE"));
        statement.setString(227, value.getAs("CVV_RESULT_CODE"));
        statement.setString(228, value.getAs("SETTLEMENT_CURENCY_CODE"));
        if (StringUtils.isBlank(value.getAs(("SETTLEMENT_AMOUNT")))) {
            statement.setString(229, "".toString());
        } else {
            statement.setBigDecimal(229, BigDecimal.valueOf(Double.parseDouble(value.getAs("SETTLEMENT_AMOUNT"))));
        }
        if (StringUtils.isBlank(value.getAs(("SETTLEMENT_USD_AMOUNT")))) {
            statement.setString(230, "".toString());
        } else {
            statement.setBigDecimal(230, BigDecimal.valueOf(Double.parseDouble(value.getAs("SETTLEMENT_USD_AMOUNT"))));
        }
        if (StringUtils.isBlank(value.getAs(("SETTLEMENT_FN_AMOUNT")))) {
            statement.setString(231, "".toString());
        } else {
            statement.setBigDecimal(231, BigDecimal.valueOf(Double.parseDouble(value.getAs("SETTLEMENT_FN_AMOUNT"))));
        }
        if (StringUtils.isBlank(value.getAs(("SETTLEMENT_CONVERSION_RATE")))) {
            statement.setString(232, "".toString());
        } else {
            statement.setBigDecimal(232,
                    BigDecimal.valueOf(Double.parseDouble(value.getAs("SETTLEMENT_CONVERSION_RATE"))));
        }
        if (StringUtils.isBlank(value.getAs(("TIME_SETTLED_MS")))) {
            statement.setString(233, "".toString());
        } else {
            statement.setLong(233, Long.parseLong(value.getAs("TIME_SETTLED_MS")));
        }
        if (StringUtils.isBlank(value.getAs(("TIME_FUNDING_GUARANTEED_MS")))) {
            statement.setString(234, "".toString());
        } else {
            statement.setLong(234, Long.parseLong(value.getAs("TIME_FUNDING_GUARANTEED_MS")));
        }
        if (StringUtils.isBlank(value.getAs(("FUNDING_GUARANTEE_TIME_UNITS")))) {
            statement.setString(235, "".toString());
        } else {
            statement.setLong(235, Long.parseLong(value.getAs("FUNDING_GUARANTEE_TIME_UNITS")));
        }

        statement.setString(236, value.getAs("TIME_UNIT_OF_MEASURE_CODE"));
        if (StringUtils.isBlank(value.getAs(("TIME_CAPTURED_MS")))) {
            statement.setString(237, "".toString());
        } else {
            statement.setLong(237, Long.parseLong(value.getAs("TIME_CAPTURED_MS")));
        }
        statement.setString(238, value.getAs("MERCHANT_ID"));
        statement.setString(239, value.getAs("SECONDARY_MERCHANT_ID"));
        if (StringUtils.isBlank(value.getAs(("INDUSTRY_CLASS_NUMBER")))) {
            statement.setString(240, "".toString());
        } else {
            statement.setLong(240, Long.parseLong(value.getAs("INDUSTRY_CLASS_NUMBER")));
        }
        if (StringUtils.isBlank(value.getAs(("INDUSTRY_SUBCLASS_NUMBER")))) {
            statement.setString(241, "".toString());
        } else {
            statement.setLong(241, Long.parseLong(value.getAs("INDUSTRY_SUBCLASS_NUMBER")));
        }
        statement.setString(242, value.getAs("SOFT_DESCRIPTOR"));
        statement.setString(243, value.getAs("RATE_CODE"));
        statement.setString(244, value.getAs("IS_INTERNATIONAL_USAGE"));
        statement.setString(245, value.getAs("IS_PLACEHOLDER"));
        statement.setString(246, value.getAs("IS_STAND_IN"));
        statement.setString(247, value.getAs("ON_MANDATE_HOLD"));
        statement.setString(248, value.getAs("ACCRUAL_BENEFICIARY_DESCRIPTOR"));
        statement.setString(249, value.getAs("ACCRUAL_REASON_CODE"));
        statement.setString(250, value.getAs("INTERMEDIARY_REFERENCE_NUMBER"));
        statement.setString(251, value.getAs("SETTLEMENT_REFERENCE_NUMBER"));
        statement.setString(252, value.getAs("AUTHORIZER_PARTY_KEY"));
        statement.setString(253, value.getAs("AUTHORIZATION_MATCH_TOKEN"));
        statement.setString(254, value.getAs("AUTHENTICATION_METHOD_CODE"));
        statement.setString(255, value.getAs("FEE_CURRENCY_CODE"));
        if (StringUtils.isBlank(value.getAs(("ACCRUAL_FEE")))) {
            statement.setString(256, "".toString());
        } else {
            statement.setBigDecimal(256, BigDecimal.valueOf(Double.parseDouble(value.getAs("ACCRUAL_FEE"))));
        }
        if (StringUtils.isBlank(value.getAs(("ACCRUAL_USD_FEE")))) {
            statement.setString(257, "".toString());
        } else {
            statement.setBigDecimal(257, BigDecimal.valueOf(Double.parseDouble(value.getAs("ACCRUAL_USD_FEE"))));
        }
        if (StringUtils.isBlank(value.getAs(("ACCRUAL_FN_FEE")))) {
            statement.setString(258, "".toString());
        } else {
            statement.setBigDecimal(258, BigDecimal.valueOf(Double.parseDouble(value.getAs("ACCRUAL_FN_FEE"))));
        }
        if (StringUtils.isBlank(value.getAs(("DISBURSAL_DAYS")))) {
            statement.setString(259, "".toString());
        } else {
            statement.setLong(259, Long.parseLong(value.getAs("DISBURSAL_DAYS")));
        }
        statement.setString(260, value.getAs("CLEARING_REQUEST_PARTY_KEY"));
        statement.setString(261, value.getAs("LOAN_PAYMENT_TYPE_CODE"));
        statement.setString(262, value.getAs("LOAN_PAYMENT_STATUS_CODE"));
        statement.setString(263, value.getAs("ORDER_NUMBER"));
        statement.setString(264, value.getAs("PARTNER_RESPONSE_CODE"));
        statement.setString(265, value.getAs("INTERNAL_RESPONSE_CODE"));
        statement.setString(266, value.getAs("AUTHORIZATION_TOKEN_UUID"));
        if (StringUtils.isBlank(value.getAs(("VALUE_TO_USD_RATE")))) {
            statement.setString(267, "".toString());
        } else {
            statement.setLong(267, Long.parseLong(value.getAs("VALUE_TO_USD_RATE")));
        }
        statement.setString(268, value.getAs("ACQUIRER_PARTY_KEY"));
        statement.setString(269, value.getAs("TAX_CURRENCY_CODE"));
        statement.setString(270, value.getAs("PAYOFF_ACCOUNT_CLASS_CODE"));
        statement.setString(271, value.getAs("PAYOFF_ACCOUNT_KEY"));
        if (StringUtils.isBlank(value.getAs(("LOAN_FEE")))) {
            statement.setString(272, "".toString());
        } else {
            statement.setBigDecimal(272, BigDecimal.valueOf(Double.parseDouble(value.getAs("LOAN_FEE"))));
        }
        if (StringUtils.isBlank(value.getAs(("LOAN_USD_FEE")))) {
            statement.setString(273, "".toString());
        } else {
            statement.setBigDecimal(273, BigDecimal.valueOf(Double.parseDouble(value.getAs("LOAN_USD_FEE"))));
        }
        if (StringUtils.isBlank(value.getAs(("LOAN_FN_FEE")))) {
            statement.setString(274, "".toString());
        } else {
            statement.setBigDecimal(274, BigDecimal.valueOf(Double.parseDouble(value.getAs("LOAN_FN_FEE"))));
        }
        statement.setString(275, value.getAs("LOAN_ACTION_PROCESSING_CODE"));
        if (StringUtils.isBlank(value.getAs(("SNAPSHOT_TYPE_NUMBER")))) {
            statement.setString(276, "".toString());
        } else {
            statement.setLong(276, Long.parseLong(value.getAs("SNAPSHOT_TYPE_NUMBER")));
        }
        statement.setString(277, value.getAs("LOAN_USAGE_CODE"));
        statement.setString(278, value.getAs("PROCESSING_END_POINT_CODE"));
        if (StringUtils.isBlank(value.getAs(("FORM_FACTOR_FEATURE_KEY")))) {
            statement.setString(279, "".toString());
        } else {
            statement.setLong(279, Long.parseLong(value.getAs("FORM_FACTOR_FEATURE_KEY")));
        }
        statement.setString(280, value.getAs("PAYMENT_ATTRIBUTES"));
        statement.setString(281, value.getAs("WAS_EXTERNALLY_CONVERTED"));
        statement.setString(282, value.getAs("TENANT_PAYMENT_MEMO"));
        statement.setString(283, value.getAs("PARENT_PAYMENT_EVENT_KEY"));
        statement.setString(284, value.getAs("LEGAL_PARTY_KEY"));
        statement.setString(285, value.getAs("TENANT_PARTY_KEY"));
        if (StringUtils.isBlank(value.getAs(("DATA_SOURCE_ID")))) {
            statement.setString(286, "".toString());
        } else {
            statement.setLong(286, Long.parseLong(value.getAs("DATA_SOURCE_ID")));
        }
        statement.setString(287, value.getAs("OPERATIONAL_TYPE_CODE")); //has to remove
        statement.setString(288, value.getAs("TIME_INITIATED_MS"));

        return statement;
    }
}
