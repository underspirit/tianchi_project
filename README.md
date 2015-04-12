# tianchi_project

## 项目结构

- **data_preprocess/** 数据预处理模块，包括将原始数据导入数据库（MySQL、MongoDB）的函数

- **user_modeling/** 用户建模模块，实现用户对商品的评分、用户购买行为分析等操作

- **recommend/** 商品推荐、购买预测模块

- **data/** 存储处理结果

- **log/** 存储日志

- **conf/** 存储配置文件

___

### data/ 结果文件说明

├── popularity_desire_behaviorRate_data.csv
├── positive_userset_2015-04-10-17-43-38.json
├── positive_userset_2015-04-12-14-32-11.csv
├── result                                  存放推荐结果
│   ├── UserCF_recommend_3.csv
│   ├── UserCF_recommend_3_intersect.csv
│   ├── UserCF_recommend.csv
│   └── UserCF_recommend_intersect.csv
├── split_train_test_sets                   存放测试集和训练集
│   ├── test_set.csv
│   └── train_set.csv
├── tianchi_mobile_recommend_train_item.csv 天池提供的商品集
└── user_view_item_num.csv                  用户浏览商品数的结果

