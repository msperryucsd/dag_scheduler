from mpi4py import MPI
import time
import numpy as np
from dag_parallelizer import create_csdl_like_graph, assign_costs, Scheduler
from dag_parallelizer.schedulers import MTA, MTA_ETA, MTA_PT2PT_INSERTION, MTA_PT2PT_PRE_RECV, MTA_PT2PT_ARB, MTA_PT2PT_ARJIT, MTA_PT2PT_ETA_BUT_NB
from dag_parallelizer.compiler.generator import code_generator
from dag_parallelizer.compiler.run_code import run_code 
# from dag_parallelizer import create_csdl_like_graph, schedule_graph, compile_schedule, execute
# from dag_parallelizer.schedulers import random, metis, lns, lns_old

import os

def create_directory(directory_name):
    if not os.path.exists(directory_name):
        os.makedirs(directory_name)
        print(f"Directory '{directory_name}' created successfully.")
    else:
        print(f"Directory '{directory_name}' already exists.")

if __name__ == '__main__':
    # MPI:
    comm = MPI.COMM_WORLD
    rank = comm.rank

    # Graph creation settings:
    SUPER_OUTPUT = 0


    # graph_size_list = [20,30,100,200,1000,2000,10000,20000,100000,500000]
    graph_size_list = [10, 30, 100, 500, 1000, 10000, 100000]
    structure_list = ['embarassing', 'embarassing diamond', 'random']
    structure_names = {
        'embarassing': 'e',
        'embarassing diamond': 'ed',
        'random': 'r',
    }

    for graph_size in graph_size_list:
        for structure in structure_list:
            if structure == 'random':
                for num in range(7):        
                    graph_name = f'{str(graph_size)}_{structure_names[structure]}_{str(num)}'
                    create_directory(graph_name)
                    ccl_graph = create_csdl_like_graph(
                        comm,
                        graph_size = graph_size,
                        super_output = SUPER_OUTPUT,
                        structure = structure,
                        save = f'{graph_name}/{graph_name}',
                    )
            else:
                for num in range(6):        
                    num = num+1
                    graph_name = f'{str(graph_size)}_{structure_names[structure]}_{str(num)}'
                    create_directory(graph_name)
                    ccl_graph = create_csdl_like_graph(
                        comm,
                        graph_size = graph_size,
                        super_output = SUPER_OUTPUT,
                        structure = structure,
                        save = f'{graph_name}/{graph_name}',
                        num_paths=num
                    )