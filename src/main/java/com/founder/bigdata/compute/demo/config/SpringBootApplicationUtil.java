package com.founder.bigdata.compute.demo.config;


import cn.hutool.extra.spring.SpringUtil;
import lombok.extern.slf4j.Slf4j;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.data.redis.RedisProperties;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Import;
import org.springframework.core.env.*;


@SpringBootApplication(scanBasePackages = {"com.founder"})
@Import(SpringUtil.class)
@Slf4j
@EnableConfigurationProperties({ RedisProperties.class})
public class SpringBootApplicationUtil {

    static SpringApplication springBootApplication = null;
    static SpringApplicationBuilder springApplicationBuilder = null;


    public static synchronized void run(String[] arge) {
        if (springBootApplication == null) {
            StandardEnvironment standardEnvironment = new StandardEnvironment();
            MutablePropertySources propertySources = standardEnvironment.getPropertySources();
            propertySources.addFirst(new SimpleCommandLinePropertySource(arge));
            String startJarPath = SpringBootApplicationUtil.class.getResource("/").getPath().split("!")[0];
            String[] activeProfiles = standardEnvironment.getActiveProfiles();
            propertySources.addLast(new MapPropertySource("systemProperties", standardEnvironment.getSystemProperties()));
            propertySources.addLast(new SystemEnvironmentPropertySource("systemEnvironment", standardEnvironment.getSystemEnvironment()));
            if (springBootApplication == null) {
                springApplicationBuilder = new SpringApplicationBuilder(SpringBootApplicationUtil.class);
                // 这里可以通过命令行传入
                springApplicationBuilder.profiles("dev");
                springApplicationBuilder.sources(SpringBootApplicationUtil.class).web(WebApplicationType.NONE);
            }
            springBootApplication = springApplicationBuilder.build();
            springBootApplication.run(arge);
        }
    }


}
