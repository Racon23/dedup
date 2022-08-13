# Descripsion
replace same file with hardlink

will use multicore to check md5

tail -f /mnt/user/data/my/dedup_log.txt

# Usage
python3 dedup.py [path] [-s -d]

python3 dedup.py

python3 dedup.py /mnt/user/data -r

python3 dedup.py /mnt/user/data -s -r

path    default is current path 

-s,-scan    scan all files and output the filepaths to file

-d,-dryrun  just check files,do not replace

-r,-recursive   include subdirectories

