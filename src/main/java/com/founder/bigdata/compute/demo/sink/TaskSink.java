package com.founder.bigdata.compute.demo.sink;

import cn.hutool.extra.spring.SpringUtil;
import com.alibaba.fastjson.JSON;
import com.founder.bigdata.compute.demo.config.SpringBootApplicationUtil;
import com.founder.bigdata.compute.demo.po.StatisticsTaskPo;
import com.founder.bigdata.compute.demo.service.IStatisticService;
import com.founder.bigdata.compute.demo.service.impl.StatisticServiceImpl;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@PublicEvolving
public class TaskSink extends RichSinkFunction<StatisticsTaskPo> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(TaskSink.class);

//    private IStatisticService statisticService;

    @Override
    public void open(Configuration parameters) throws Exception {
//        super.open(parameters);
//        SpringBootApplicationUtil.run(parameters.getString("arge", "").split(" "));
//        statisticService = SpringUtil.getBean(StatisticServiceImpl.class);
    }

    @Override
    public void invoke(StatisticsTaskPo statisticsTaskPo, Context context) {
        LOG.info(JSON.toJSONString(statisticsTaskPo));
//        statisticService.updateTaskByReportTimeAndTaskId(statisticsTaskPo);
    }
}
