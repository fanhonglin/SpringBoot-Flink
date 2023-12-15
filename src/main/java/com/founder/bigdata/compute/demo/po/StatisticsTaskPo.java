package com.founder.bigdata.compute.demo.po;

import com.baomidou.mybatisplus.annotation.TableName;
import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.util.Date;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@TableName("sqt_statistics_task")
public class StatisticsTaskPo {
    /**
     * 主键id
     */
    private Long id;
    /**
     * 任务名称
     */
    private String name;
    /**
     * 任务量
     */
    private String taskId;

    /**
     * 质检录音量
     */
    private Long qualityTotalNum;

    /**
     * 质检成功量
     */
    private Long successTotalNum;
    /**
     * 质检成功时长
     */
    private BigDecimal totalTime;

    /**
     * 质检合格量
     */
    private Long eligibleTotalNum;

    /**
     * 质检违规量
     */
    private Long illegalTotalNum;


    /**
     * 质检得分
     */
    private BigDecimal qualityTotalScore;

    /**
     * 统计日期-年-月-日
     */
    private Date reportTime;

    /**
     * 创建时间
     */
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss", timezone = "GMT+8")
    private Date createTime;

    /**
     * 更新时间
     */
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss", timezone = "GMT+8")
    private Date updateTime;
}
