# PBS 任务后处理

监听pbs database的任务结束事件，将任务信息派发给后续成程序处理

**需要postgresql版本在10以上**

### 功能

- 提供任务完成的通知
- 汇总任务生成的的数据并导出到其他数据库, 这些数据包括
  - PBS的任务信息
  - 从ipeople-db中获得的用户额外信息，包括邮件，部门等
  - 从监控中获得的性能时间序列信息经过统计后的标量结果

### TODO
  - 根据任务的文件中获得的额外信息， 比如模型网格数，每步的时长等
