# 作业考核点完成情况（4、11、12填写具体值，其他都填是或否）
1. 是否支持本机一键启动服务：是
2. 是否支持分布式环境下的一键启动：是
3. 根据配置参数，是否能输出正确计算结果：是
4. 实现的 FileFormat 接口类：UnsplitFileFormat
5. FileFormat 实现类中的 isSplitable 是否返回 true：否
6. 是否正确输出 shuffle 文件及其路径：是
7. shuffle 文件格式是否为 kyro：否
8. Map 任务是否流式读取文件：是
9. Reduce 任务是否流式读取 shuffle 文件：是
10. 资源不足时，调度是否正常阻塞等待：是
11. 执行的 map 任务数量：一个任务执行了两次map和shuffle，mapstage数量为2，第一次的Map的task数量input的文件数一样，第二次map的task数量与配置文件urltopn.conf文件的reduceTask数量一样
12. 执行的 reduce 任务数量：一次任务执行了两次reduce阶段，reducestag数量为2，第一次的reduce的task数量与配置文件urltopn.conf文件的reduceTask数量一样，第二次reduce数量为1，最后生成一个文本文件
13. Master/Driver 是否参与实际计算：否
14. 结果文件是否采用 avro 格式存储：否
15. 是否提供了 Thrift 服务功能：是
16. Thrift 服务接口是否实现完整并能返回正确结果：是，获取正确结果得等文件执行完后