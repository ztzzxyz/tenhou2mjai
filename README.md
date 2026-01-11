## 准备数据集
### 1. 从[天凤官方](https://tenhou.net/sc/raw/)下载日志，使用[houou-logs](https://github.com/Apricot-S/houou-logs)转换为mjlog-xml
### 2. (可选)使用本仓库中`delete.py`过滤掉玩家离线的日志
### 3. 使用[mjlog2json](https://github.com/tsubakisakura/mjlog2json)将mjlog-xml转换为tenhou-json
### 4. 使用本仓库中`main.rs`替换[mjai-reviewer](https://github.com/Equim-chan/mjai-reviewer)中的`main.rs`，重新编译后，使用命令将tenhou-json批量转换为mjai-json
```shell
mjai-reviewer -h
Usage: mjai-reviewer <input_directory> <output_directory>
```
### 5. 使用本仓库中`compress_json_by_date.py`将mjai-json压缩为json.gz格式，并按`年/月/日`保存到对应目录
```shell
python compress_json_by_date.py -h
usage: compress_json_by_date.py [-h] input_path output_path

将指定目录中的JSON文件按日期直接压缩为gzip格式（不解析JSON内容）

positional arguments:
  input_path   输入目录路径
  output_path  输出目录路径

options:
  -h, --help   show this help message and exit
```
### 6. (可选)使用[Mortal](https://github.com/Equim-chan/Mortal)中validate_logs工具验证格式。
```shell
validate_logs {{文件或数据集目录}}
```
