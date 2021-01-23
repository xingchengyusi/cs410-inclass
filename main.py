#!/usr/bin/env python3
from producer import main as pmain
from consumer import main as cmain
from multiprocessing import Process
import ccloud_lib as ccloud


def main():
    # how many processes run at same time.
    pnum = 2
    cnum = 2
    plist = []

    for i in range(pnum):
        p = Process(target=pmain)
        p.start()
        plist.append(p)

    for j in range(cnum):
        c = Process(target=cmain)
        c.start()
        plist.append(c)

    for p in range(len(plist)):
        plist[p].join()


if __name__ == "__main__":
    main()
