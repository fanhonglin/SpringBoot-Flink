package com.founder.bigdata.compute.demo.dao;


import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.founder.bigdata.compute.demo.po.StatisticsTaskPo;
import org.apache.ibatis.annotations.Mapper;

@Mapper
public interface StatisticMapper extends BaseMapper<StatisticsTaskPo> {

}
