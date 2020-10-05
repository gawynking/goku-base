
# 一 Spring Boot工程创建

Spring Boot官网：https://spring.io/projects/spring-boot
https://spring.io/guides/gs/spring-boot/

1 IDEA创建maven工程

2 导入Spring Boot相关依赖 
参考pom.xml文件配置 

3 编写Sring Boot主程序
参考：com.pgman.goku.MySpringBootApplication 

4、接下来，可以进行业务逻辑开发了。

5、maven打包（pom文件需要添加打包插件）：
mvn clean package -Dmaven.test.skip=true
然后可以通过java -jar来执行程序 


# 二 idea集成springboot热部署：
1 settings -> compiler -> build project automatically 
2 shift+ctrl+alt+/ -> registry -> compiler.automake.allow.when.app.running 


