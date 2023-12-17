package com.founder.bigdata.compute.demo.service.impl;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.founder.bigdata.compute.demo.dao.StatisticMapper;
import com.founder.bigdata.compute.demo.po.StatisticsTaskPo;
import com.founder.bigdata.compute.demo.service.IStatisticService;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.util.Date;
import java.util.Objects;

@Service
public class StatisticServiceImpl implements IStatisticService {

    @Resource
    private StatisticMapper statisticMapper;

    @Override
    @Transactional(rollbackFor = Exception.class)
    public void updateByReportTimeAndTaskId(StatisticsTaskPo statisticsTaskPo) {

        StatisticsTaskPo statisticsTaskPoDBs = statisticMapper.selectOne(new LambdaQueryWrapper<StatisticsTaskPo>().eq(StatisticsTaskPo::getReportTime, statisticsTaskPo.getReportTime()).eq(StatisticsTaskPo::getTaskId, statisticsTaskPo.getTaskId()));
        if (Objects.isNull(statisticsTaskPoDBs)) {
            statisticsTaskPo.setCreateTime(new Date());
            statisticsTaskPo.setUpdateTime(new Date());
            statisticMapper.insert(statisticsTaskPo);
        } else {
            statisticMapper.update(statisticsTaskPo, new LambdaQueryWrapper<StatisticsTaskPo>().eq(StatisticsTaskPo::getReportTime, statisticsTaskPo.getReportTime()).eq(StatisticsTaskPo::getTaskId, statisticsTaskPo.getTaskId()));
        }
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public void updateTaskByReportTimeAndTaskId(StatisticsTaskPo statisticsTaskPo) {
        StatisticsTaskPo statisticsTaskPoDBs = statisticMapper.selectOne(new LambdaQueryWrapper<StatisticsTaskPo>().eq(StatisticsTaskPo::getReportTime, statisticsTaskPo.getReportTime()).eq(StatisticsTaskPo::getTaskId, statisticsTaskPo.getTaskId()));
        if (Objects.isNull(statisticsTaskPoDBs)) {
            statisticsTaskPo.setCreateTime(new Date());
            statisticsTaskPo.setUpdateTime(new Date());
            statisticMapper.insert(statisticsTaskPo);
        } else {
            statisticMapper.update(statisticsTaskPo, new LambdaQueryWrapper<StatisticsTaskPo>().eq(StatisticsTaskPo::getReportTime, statisticsTaskPo.getReportTime()).eq(StatisticsTaskPo::getTaskId, statisticsTaskPo.getTaskId()));
        }
    }
}
