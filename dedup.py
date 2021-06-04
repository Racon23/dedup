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
# small = False
# size = 1024*1024*2
recursive = False
idx = 1



# 扫描目录下所有文件
def scan_file(rootDir,filelist):
    if recursive:
        for root, dirs, files in os.walk(rootDir):
            for file in files:
                filelist.append(os.path.join(root, file))
    else:
        for file in os.listdir(rootDir):
            fp = os.path.join(rootDir,file)
            if os.path.isfile(fp):
                filelist.append(fp)


# 计算md5

def async_md5(filepath, result_dict, result_lock, arg):
    check = hashlib.md5()
    # sz = os.path.getsize(filepath)
    sz = 0
    length = arg['length']
    dryrun = arg['dryrun']

    with open(filepath, "rb") as fp:
        while True:
            # 1M 分块读取，簇大小一般4k，根据自己喜好修改
            data = fp.read(1048576)
            if not data:
                break
            check.update(data)
            sz = sz + len(data)
    
    nsz = ''
    KB = 1024
    MB = 1024*1024
    GB = 1024*1024*1024
    if sz>GB:
        sz = sz/GB
        nsz = str(round(sz, 2))+ 'GB'
    elif sz>MB:
        sz = sz/MB
        nsz = str(round(sz, 2)) + 'MB'
    elif sz>KB:
        sz = sz/KB
        nsz = str(round(sz, 2)) + 'KB'
    else:
        nsz = str(sz)+'B'

    file_md5 = check.hexdigest()
    with result_lock:
        per = arg['per']+1
        arg['per']=per
        # pid = os.getpid()
        curper = str(per)+'/'+str(length)
        outlog = open("./dedup_log.txt", "a", encoding="utf8")
        if result_dict.get(file_md5) == None:
            result_dict[file_md5] = filepath
            print(curper+' - '+ file_md5+" - " + nsz + " - " + filepath)
            outlog.write(curper+' - '+file_md5+" - "+nsz+" - " + filepath+'\n')
        else:
            print(curper+' - '+"exist! " + nsz+" - " + result_dict[file_md5]+" - "+filepath)
            outlog.write(curper + " - "+ "exist! "+nsz+" - " +
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
    cores = mp.cpu_count()
    manager = mp.Manager()
    managed_locker = manager.Lock()
    managed_dict = manager.dict()
    managed_arg = manager.dict()
    pool = mp.Pool(cores)
    # enumerate 同时获取下标和值
    managed_arg['length'] = len(filelist)
    managed_arg['dryrun'] = dryrun
    managed_arg['per'] = 0
    results = [pool.apply_async(async_md5, args=(
        filepath, managed_dict, managed_locker, managed_arg))for filepath in filelist]
    results = [p.get() for p in results]


def run(rootDir):
    filelist = []
    scan_file(rootDir,filelist)
    if scan:
        for file in filelist:
            print(file)
            of.write(file+'\n')
    else:
        multicore(filelist)
    print(len(filelist))

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
        elif sys.argv[idx].strip('-') in ('recursive', 'r'):
            recursive = True
            idx = idx+1
        elif sys.argv[idx].strip('-') in ('small', 'l'):
            small = True
            idx = idx+1
        elif sys.argv[idx].strip('-') in ('interactive', 'i'):
            interactive = True
            idx = idx+1

        else:
            print('no such arg!\n\n'+
            'python3 dedup.py [path] [-s -d]\n'+
            'python3 dedup.py /mnt/user/data -r\n\n'+
            'path    default is current path \n'+
            '-s,-scan    scan all files and output the filepaths to file\n'+
            '-d,-dryrun  just check files,do not replace\n'+
            '-r,-recursive   include subdirectories\n')
            sys.exit(-1)
    # scan_file(path)
    run(path)
    sys.exit(-1)
