# handle csv by rust   [![Rust Report Card](https://rust-reportcard.xuri.me/badge/github.com/likongyang/handle_csv)](https://rust-reportcard.xuri.me/report/github.com/likongyang/handle_csv)
#### 对csv文件之间的处理，含补集、并集、交集指定关键字在列中的搜索，最终生成新的文件
功能说明：
- 单个csv文件和一个汇总文件之间的补集（即汇总文件中不含单个csv文件中的条目）
- 多个csv文件的补集（即生成不具备重复的汇总文件）
- 多个csv文件的交集(即多个文件之间都有的条目)
- 对指定文件中的指定列搜索关键字，并生成关联条目的csv文件
- 备注：支持单个对多个以及多个对多个的csv文件操作，具体在于使用的逻辑
#### 更新说明
- 更新为hashmap的算法，以使复杂度降为O(N)
