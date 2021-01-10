#!/usr/bin/env python3

import sys
import os
import argparse

guser = os.environ['GCP_userID']

parser = argparse.ArgumentParser()

parser.add_argument('-f', '--file', dest='cfile', help='source file', type=str)
parser.add_argument('-n', '--np', dest='np', help='number of MPI processes', type=int)


options = parser.parse_args()

if options.cfile is None:
    parser.print_usage()
    sys.exit()

if not os.path.isfile(options.cfile):
    print("Source file does not exist")
    parser.print_usage()
    sys.exit()

IPs= []

sourcefile=os.path.basename(options.cfile)
execfile=sourcefile.replace(".c",".run")

print("\n ### collect list of IP addresses ###")

with open('hosts') as hostfile:
    record_ip=False
    for line in hostfile:
        clean_line=line.rstrip()
        if clean_line == "[mpi_nodes]":
            record_ip = True
        elif record_ip:
            if not clean_line:
                break
            else:
                print(clean_line)
                IPs.append(clean_line)

# copy source file to all nodes
print("\n ### copy source code to node {} and compile ###".format(IPs[0]))

os.system("scp -i {0} {1} {2}@{3}:./".format("buffer/id_rsa", options.cfile, guser, IPs[0]))
    
# Compile on first node
os.system("ssh -i {0} {1}@{2} \"make\"".format("buffer/id_rsa", guser, IPs[0]))


# Copy exec to all nodes
print("\n ### copy executable file to all nodes ###")

for IP in IPs:
    if IP != IPs[0]:
        os.system("ssh -i {0} {1}@{2} \"scp {3} {4}:./ \"".format("buffer/id_rsa", guser, IPs[0], execfile, IP))

if options.np is None:
    nb_ps = len(IPs)
else:
    nb_ps = options.np

# Exec the mpi program
print("\n ### Execution of the MPI program with {} processes ###".format(nb_ps))

os.system("ssh -i {0} {1}@{2} \"mpirun -n {3} --hostfile hostfile_mpi ./{4}\"".format("buffer/id_rsa", guser, IPs[0], nb_ps, execfile))
