# 描述
用多核计算md5,然后用硬链接替换重复文件。

tail -f /mnt/user/data/my/dedup_log.txt

# 用法
python3 dedup.py [path] [-s -d]

python3 dedup.py

python3 dedup.py /mnt/user/data -r

python3 dedup.py /mnt/user/data -s -r

path 默认路径是当前路径

-s,-scan 扫描所有文件和路径并输出到文件中

-d,-dryrun 只检查md5，不修改

-r,-recursive 包括子目录
