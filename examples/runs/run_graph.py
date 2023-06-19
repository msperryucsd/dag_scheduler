from mpi4py import MPI
import time
import numpy as np
from dag_parallelizer import create_csdl_like_graph, assign_costs, Scheduler, normalize_costs
from dag_parallelizer.schedulers import MTA, MTA_ETA, MTA_PT2PT_INSERTION, MTA_PT2PT_PRE_RECV, MTA_PT2PT_ARB, MTA_PT2PT_ARJIT, MTA_PT2PT_ETA_BUT_NB, MTB_ETA, MTB_ETA_ARB
from dag_parallelizer.compiler.generator import code_generator
from dag_parallelizer.compiler.run_code import run_code 
import sys
# from dag_parallelizer import create_csdl_like_graph, schedule_graph, compile_schedule, execute
# from dag_parallelizer.schedulers import random, metis, lns, lns_old

# Graph partitioning:
# PARTITION_TYPE = MTA()
# PARTITION_TYPE = MTA_PT2PT_INSERTION()
# PARTITION_TYPE = MTA_PT2PT_PRE_RECV()
# PARTITION_TYPE = MTA_ETA() # Blocking
# PARTITION_TYPE = MTA_PT2PT_ARB() # Non-blocking
# PARTITION_TYPE = MTA_PT2PT_ARJIT()
# PARTITION_TYPE = MTA_PT2PT_ETA_BUT_NB()

if __name__ == '__main__':
    # MPI:
    comm = MPI.COMM_WORLD
    comm_size = comm.size
    rank = comm.rank

    # Graph creation settings:
    # LOAD_GRAPH = None
    # LOAD_GRAPH = 'graph'

    # Operation settings:
    # VARIABLE_SHAPE = (20, 15, 5, 1, 10)
    # VARIABLE_SHAPE = (2000,)
    # # VARIABLE_SHAPE = (1)
    # VARIABLE_SIZE = np.prod(VARIABLE_SHAPE)
    # COMM_COST = VARIABLE_SIZE/100
    # OP_COST = VARIABLE_SIZE/100
    # # COMM_COST = OP_COST*30
    # # COMM_COST = OP_COST
    # COMM_COST = 0.05
    # OP_COST = 0.05


    if len(sys.argv) > 1:
        graph_name = sys.argv[1]
        print("Load name:", graph_name)
    else:
        print("No argument provided.")

    mta_eta = MTA_ETA()
    mta_pt2pt_arb = MTA_PT2PT_ARB()
    mtb_eta = MTB_ETA() # Blocking Static T Level
    mtb_eta_arb = MTB_ETA_ARB() # Non-blocking
    partition_types = [mta_eta, mta_pt2pt_arb, mtb_eta, mtb_eta_arb]
    partition_type_names = {
        mta_eta: 'Static B-Level',
        mta_pt2pt_arb: 'Static B-Level (Non-blocking)',
        mtb_eta: 'Static T-Level',
        mtb_eta_arb: 'Static T-Level (Non-blocking)',
    } 
    variable_sizes = [1, 10000]
    graph_dir =  f'graphs/{graph_name}'
    load_graph = f'graphs/{graph_name}/{graph_name}'

    # For later analysis
    PROFILE = 0
    MAKE_PLOTS = 0
    VISUALIZE_SCHEDULE = 0

    # Run settings:
    NUM_RUNS = 1

    save_dict = {}
    for variable_size in variable_sizes:
        save_dict[variable_size] = {}
        for partition_type in partition_types:
            pname = partition_type_names[partition_type]
            save_dict[variable_size][pname] = {}
            ccl_graph = create_csdl_like_graph(
                comm,
                graph_size = 0,
                super_output = 0,
                load_graph= load_graph,
            )


            # Normalize costs so it doesn't take too long to run.
            normalize_costs(
                ccl_graph,
                max_time = 1,
                communication_cost = 0.00005,
            )
            save_dict[variable_size][pname]['max_time'] = 1
            save_dict[variable_size][pname]['communication_cost'] = 0.00005
            # continue

            # Create a schedule from a choice of algorithms
            scheduler = Scheduler(partition_type, comm)
            schedule = scheduler.schedule(
                ccl_graph,
                profile = PROFILE,
                create_plots = MAKE_PLOTS,
                visualize_schedule = VISUALIZE_SCHEDULE,
            )

            code_object, num_coms, num_ops, num_coms_pre = code_generator(
                schedule,
                ccl_graph,
                variable_size,
                rank,
                save = 0,
                )
            if rank == 0:
                s = time.time()
            # import cProfile
            # profiler = cProfile.Profile()
            # profiler.enable()
            run_code(code_object, ccl_graph, rank, NUM_RUNS, variable_size, num_coms, num_ops, num_coms_pre)
            # profiler.disable()
            # profiler.dump_stats(f'output_{comm.rank}')

            if rank == 0:
                final_time = time.time() - s
                print('\nFINAL TIME:', final_time)
                save_dict[variable_size][pname]['final_time'] = final_time

                import pickle
                import os
                pickle_file_path = f'{graph_dir}/stats.pickle'
                # Check if the pickle file exists
                if os.path.exists(pickle_file_path):
                    with open(pickle_file_path, 'rb') as handle:
                        full = pickle.load(handle)
                else:
                    full = {}
                
                full[comm_size] = save_dict

                with open(pickle_file_path, 'wb') as handle:
                    pickle.dump(full, handle, protocol=pickle.HIGHEST_PROTOCOL)


