import numpy as np
import pandas as pd 

with open("log.txt") as logfile:
    in_entry = False
    entries = []
    entry = []

    for line in logfile:
        l = line.strip()

        if l == "<entry>":
            in_entry = True
            entry = []
            continue
        elif l == "</entry>":
            in_entry = False
            entries.append(np.array(entry).astype(np.float))
            continue

        if in_entry:
            t = l.split(": ")
            if (len(t) < 2):
                continue
            entry.append(t[1].split(" ")[0])
    
    entry_count = len(entries)
    entries_avg = np.add.reduce(entries) / entry_count
    
    for e in entries_avg:
        print(e, end=",")
    print()
