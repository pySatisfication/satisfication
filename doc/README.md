### 指标说明
#### 顾比策略
位置1:
+ &ensp;1: `boll_st_s1`大于等于`boll_st_s5`
+ -1: `boll_st_s1`小于`boll_st_s5` </br>
位置2:</br>
+ &ensp;1: `boll_st_s1`上穿`boll_st_s5`
+ &ensp;0: 未交叉
+ -1: `boll_st_s1`下穿`boll_st_s5`

#### 背离
```
位置1~4: 分别表示是否MACD底背离、DIFF底背离、MACD柱背离、MACD柱面积底背离（1: 是, 0:否）
位置5~8: 分别表示是否MACD顶背离、DIFF顶背离、MACD顶背离、MACD柱面积顶背离（1: 是, 0:否）
```
#### 分型
+ &ensp;1: 顶分型
+ &ensp;0: 非分型点
+ -1: 底分型

#### ADX极值
+ &ensp;1: adx上穿-60.0
+ &ensp;0: 非极值点
+ -1: adx下穿60

#### 金叉死叉有效性
+ &ensp;1: 有效金叉, 多头信号
+ &ensp;0: 非叉点，或非有效金叉死叉
+ -1: 有效死叉, 空头信号
