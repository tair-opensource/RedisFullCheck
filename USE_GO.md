# 初始化项目要进入 full-check中进行这两个命令
go mod init full_check
go mod tidy

# 自动下载缺失的模块并更新 go.sum 文件
go get -t ./common

# 执行模块下的断言
go test D:\IntelliJIDEA2022.3.3\idea-project\RedisFullCheck\src\full_check\common -v
go test ./common -v
