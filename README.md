# AN6007
还可以添加的功能：register界面submit时判定该meterid是否已经存在
使用class 按职责分离，使用面向对象的方式重构。整个系统可分为如下几个部分：

目录管理（DirectoryManager）
负责创建所需的文件夹以及生成按年月组织的文件路径。

账户管理（AccountManager）
负责加载、保存账户信息，并提供注册接口。

时间管理（TimeManager）
负责读取和更新当前模拟时间。

数据采集器（ReadingGenerator）
根据当前时间、账户信息生成电表读数，并维护“最新读数”与每日缓存。

日数据处理（DailyProcessor）
将当天（或跨天）的缓存数据进行组织，并保存为 JSON 文件。

月数据归档（MonthlyProcessor）
归档上月（或更早）数据，计算月总用电量，并清理旧数据。

智能电表系统（SmartMeterSystem）
作为门面类，将上述各部分组合起来，对外提供注册、采集、重置等接口。