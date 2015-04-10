# tianchi_project

## 项目结构
- **data_preprocess/** 数据预处理模块，包括将原始数据导入数据库（MySQL、MongoDB）的函数
- **user_modeling/** 用户建模模块，实现用户对商品的评分、用户购买行为分析等操作
- **recommend/** 商品推荐、购买预测模块
- **data/** 存储处理结果
- **log/** 存储日志
- **conf/** 存储配置文件

### data/ 结果文件说明
- **UserCF_recommend.csv**                     对每个用户推荐5个商品
- **UserCF_recommend_intersect.csv**           对每个用户推荐5个商品,并与商品集取交集
- **UserCF_recommend_3.csv**                   对每个用户推荐3个商品
- **UserCF_recommend_3_intersect.csv**         对每个用户推荐3个商品,并与商品集取交集
- **tianchi_mobile_recommend_train_item.csv**  天池提供的商品集
- **user_view_item_num.csv**                   用户浏览商品数的结果
- **positive_userset.csv**                     正样本结果
