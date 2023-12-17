package com.founder.bigdata.compute.demo.service;

import com.founder.bigdata.compute.demo.po.StatisticsTaskPo;

public interface IStatisticService {

    void updateByReportTimeAndTaskId(StatisticsTaskPo statisticsTaskPo);

    void updateTaskByReportTimeAndTaskId(StatisticsTaskPo statisticsTaskPo);

}
