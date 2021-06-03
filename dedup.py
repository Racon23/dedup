import sys
import os
import hashlib
# import redis
import multiprocessing as mp

of = open("./filelist.txt", "w", encoding="utf8")
dict = {}
dryrun = False
scan = False
interactive = False
small = False
size = 1024*1024*2
idx = 1
cores = mp.cpu_count()


# 扫描目录下所有文件
def scan_file(rootDir):
    filelist = []
    for root, dirs, files in os.walk(rootDir):
        for file in files:
            # print(os.path.join(root,file))
            # of.write(os.path.join(root,file)+'\n')
            if scan == True:
                print(os.path.join(root, file))
                of.write(os.path.join(root, file)+'\n')
            else:
                filelist.append(os.path.join(root, file))
        for dir in dirs:
            scan_file(dir)
    multicore(filelist)

# 计算md5


def async_md5(filepath, result_dict, result_lock, dryrun):
    check = hashlib.md5()
    sz = os.path.getsize(filepath)
    nsz = ''
    nsz = str(sz)+'B'
    if sz>1024:
        sz = sz/1024
        nsz = str(round(sz, 2))+'KB'
    if sz>1024:
        sz = sz/1024
        nsz = str(round(sz, 2)) + 'MB'
    if sz>1024:
        sz = sz/1024
        nsz = str(round(sz, 2))+ 'GB'

    with open(filepath, "rb") as fp:
        while True:
            # 100M
            data = fp.read(104857600)
            if not data:
                break
            check.update(data)
    file_md5 = check.hexdigest()
    with result_lock:
        outlog = open("./dedup_log.txt", "a", encoding="utf8")
        if result_dict.get(file_md5) == None:
            result_dict[file_md5] = filepath
            print(file_md5+" - " + nsz + " - " + filepath)
            outlog.write(file_md5+" - "+nsz+" - " + filepath+'\n')
        else:
            print("exist! " + nsz+" - " + result_dict[file_md5]+" - "+filepath)
            outlog.write("exist! "+nsz+" - " +
                         result_dict[file_md5]+" - "+filepath+'\n')
            if dryrun == False:
                replace(result_dict[file_md5], filepath)
    return

# 替换为硬链接


def replace(oldfile, newfile):
    if(os.path.exists(newfile)):
        os.remove(newfile)
        os.link(oldfile, newfile)
    else:
        print('error.no such file!'+newfile)


def multicore(filelist):
    manager = mp.Manager()
    managed_locker = manager.Lock()
    managed_dict = manager.dict()
    pool = mp.Pool(cores)
    results = [pool.apply_async(async_md5, args=(
        filepath, managed_dict, managed_locker, dryrun))for filepath in filelist]
    results = [p.get() for p in results]


if __name__ == "__main__":
    path = '.'
    while len(sys.argv) > idx:
        # print(idx)
        if sys.argv[idx].startswith('-') == False:
            path = sys.argv[idx]
            idx = idx+1
        elif sys.argv[idx].strip('-') in ('scan', 's'):
            scan = True
            idx = idx+1
        elif sys.argv[idx].strip('-') in ('dryrun', 'd'):
            dryrun = True
            idx = idx+1
        elif sys.argv[idx].strip('-') in ('small', 'l'):
            small = True
            idx = idx+1
        elif sys.argv[idx].strip('-') in ('interactive', 'i'):
            interactive = True
            idx = idx+1

        else:
            print('no such arg')
    scan_file(path)
    sys.exit(-1)
