package com.founder.bigdata.compute.demo.service.impl;

import cn.hutool.core.date.DateUtil;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.founder.bigdata.compute.demo.dao.StatisticMapper;
import com.founder.bigdata.compute.demo.po.StatisticsTaskPo;
import com.founder.bigdata.compute.demo.service.IStatisticService;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Objects;

import static cn.hutool.core.date.DatePattern.NORM_DATE_PATTERN;

@Service
public class StatisticServiceImpl implements IStatisticService {

    @Resource
    private StatisticMapper statisticMapper;

    @Override
    @Transactional(rollbackFor = Exception.class)
    public void updateByReportTimeAndTaskId(String reportTime, String taskId, Long value) {

        StatisticsTaskPo statisticsTaskPoDBs = statisticMapper.selectOne(new LambdaQueryWrapper<StatisticsTaskPo>().eq(StatisticsTaskPo::getReportTime, reportTime).eq(StatisticsTaskPo::getTaskId, taskId));

        if (Objects.isNull(statisticsTaskPoDBs)){
            StatisticsTaskPo statisticsTaskPo = StatisticsTaskPo.builder()
                    .reportTime(DateUtil.parse(reportTime, NORM_DATE_PATTERN))
                    .createTime(new Date())
                    .updateTime(new Date())
                    .name("1234")
                    .taskId(taskId)
                    .qualityTotalNum(value).build();
            statisticMapper.insert(statisticsTaskPo);
        }else {
            statisticsTaskPoDBs.setQualityTotalNum(value);
            statisticMapper.update(statisticsTaskPoDBs, new LambdaQueryWrapper<StatisticsTaskPo>().eq(StatisticsTaskPo::getReportTime, reportTime).eq(StatisticsTaskPo::getTaskId, taskId));
        }




    }
}
