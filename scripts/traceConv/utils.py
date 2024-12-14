import os
import subprocess
import shutil
import sys


def post_process(ifilepath):
    dir_path = os.path.dirname(ifilepath)
    if not os.path.exists(dir_path + "/stat"):
        os.mkdir(dir_path + "/stat")
        os.mkdir(dir_path + "/lcs")
        os.mkdir(dir_path + "/finished")

    shutil.move(ifilepath + ".stat", f"{dir_path}/stat/")

    subprocess.run("zstd -16 --long -T16 " + ifilepath + ".lcs", shell=True)
    shutil.move(ifilepath + ".lcs.zst", f"{dir_path}/lcs/")

    subprocess.run("zstd -8 " + ifilepath, shell=True)
    os.remove(ifilepath)
    os.remove(ifilepath + ".pre_lcs")
    os.remove(ifilepath + ".lcs")

    shutil.move(ifilepath + ".zst", f"{dir_path}/finished/")
