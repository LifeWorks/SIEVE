#!/usr/bin/env python

import os
from datetime import datetime
import math


def main():
    dataDir = os.getcwd()
    outDir = dataDir + "/output"
    nodeNum = 1
    numPerNode = 16
    kmerSize = 14

    try:
        opts, args = getopt.getopt(sys.argv[1:], "hd:o:n:p:k:", [
                                   "help", "dataDir=", "outDir=", "nodeNum=", "numPerNode=", "kmerSize="])
    except getopt.GetoptError:
        print('python generateKmers.py -d <data-directory> -o <output-directory> -n <node-number> -p <thread-number-per-node> -k <kmer-size>')
        sys.exit(2)
    for opt, arg in opts:
        if opt in ('-h', "--help"):
            print(
                'python generateKmers.py -d <data-directory> -o <output-directory> -n <node-number> -p <thread-number-per-node> -k <kmer-size>')
            sys.exit()
        elif opt in ("-d", "--dataDir"):
            dataDir = arg
            if outDir == os.getcwd() + "/output":
                outDir = dataDir + "/output"
        elif opt in ('-o', "--outDir"):
            outDir = arg
        elif opt in ('-n', "--nodeNum"):
            nodeNum = int(arg)
            if nodeNum < 0:
                print('nodeNum is negative')
                sys.exit()
        elif opt in ('-p', "--numPerNode"):
            numPerNode = int(arg)
            if numPerNode < 0:
                print('numPerNode is negative')
                sys.exit()
        elif opt in ("-k", "--kmerSize"):
            kmerSize = int(arg)
        else:
            print(
                'python generateKmers.py -d <data-directory> -o <output-directory> -n <node-number> -p <thread-number-per-node> -k <kmer-size>')
            sys.exit(2)

    return(dataDir, outDir, nodeNum, numPerNode, kmerSize)


if __name__ == "__main__":
    dataDir, outDir, nodeNum, numPerNode, kmerSize = main()

# timestamp = str(datetime.now())
# outDir = outDir + '-' + timestamp
try:
    os.makedirs(outDir)
except OSError as e:
    if e.errno != os.errno.EEXIST:
        raise

files = []
for filepath in os.listdir(os.getcwd()):
    if filepath.endswith(".fasta") or filepath.endswith(".fa"):
        files.append(filepath)
sorted_files = sorted(files, key=os.path.getsize, reverse=True)
batchsize = len(files) // nodeNum + 1
sepFiles = [sorted_files[i:i+batchsize]
            for i in range(0, len(sorted_files), batchsize)]

for nodei in range(nodeNum):
    jobName = "kmer-batch-%s" % str(nodei)
    job_file = os.path.join(outDir, "%s.job" % jobName)

    job_list = os.path.join(outDir, "%s.txt" % jobName)

    with open(job_list) as fh:
        for filename in sepFiles[nodei]:
            fh.writelines("python3 KmerFeatures.py -f %s -o %s -m simple -M reduced_alphabet_0 -k %d \n" %
                          (filename, outDir, kmerSize))

    with open(job_file) as fh:
        fh.writelines("#!/bin/sh\n")
        fh.writelines("#SBATCH --account emslj50978\n")
        fh.writelines("#SBATCH --job-name %s\n" % jobName)
        fh.writelines("#SBATCH --output=.out/%s.out\n" % jobName)
        fh.writelines("#SBATCH --error=.out/%s.err\n" % jobName)
        fh.writelines("#SBATCH --time=2-00:00\n")
        fh.writelines("#SBATCH --mem=12000\n")
        fh.writelines("#SBATCH --nodes 1\n")
        fh.writelines("#SBATCH --ntasks-per-node %s\n" % str(numPerNode))
        # fh.writelines("#SBATCH --qos=\n")
        # fh.writelines("#SBATCH --mail-type=ALL\n")
        # fh.writelines("#SBATCH --mail-user=$USER@pnnl.gov\n")
        fh.writelines("module purge\n")
        fh.writelines("module load python/3.8.1\n")
        fh.writelines("parallel -j %s < %s\n" % (numPerNode, job_list))
    os.system("sbatch %s" % job_file)
