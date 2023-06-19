from mpi4py import MPI
import time
import numpy as np
from dag_parallelizer import create_csdl_like_graph, assign_costs, Scheduler
from dag_parallelizer.schedulers import MTA, MTA_ETA, MTA_PT2PT_INSERTION, MTA_PT2PT_PRE_RECV, MTA_PT2PT_ARB, MTA_PT2PT_ARJIT, MTA_PT2PT_ETA_BUT_NB, MTB_ETA, MTB_ETA_ARB
from dag_parallelizer.compiler.generator import code_generator
from dag_parallelizer.compiler.run_code import run_code 
# from dag_parallelizer import create_csdl_like_graph, schedule_graph, compile_schedule, execute
# from dag_parallelizer.schedulers import random, metis, lns, lns_old

if __name__ == '__main__':
    # MPI:
    comm = MPI.COMM_WORLD
    rank = comm.rank

    # Graph creation settings:
    LOAD_GRAPH = None
    LOAD_GRAPH = 'graph'
    GRAPH_SIZE = 1500
    SUPER_OUTPUT = 0
    # SUPER_OUTPUT = 0

    # Operation settings:
    # VARIABLE_SHAPE = (20, 15, 5, 1, 10)
    VARIABLE_SHAPE = (2000,)
    # VARIABLE_SHAPE = (1)
    VARIABLE_SIZE = np.prod(VARIABLE_SHAPE)
    COMM_COST = VARIABLE_SIZE/100
    OP_COST = VARIABLE_SIZE/100
    # COMM_COST = OP_COST*30
    # COMM_COST = OP_COST
    COMM_COST = 0.05
    OP_COST = 0.05

    # Graph partitioning:
    # PARTITION_TYPE = MTA()
    # PARTITION_TYPE = MTA_PT2PT_INSERTION()
    # PARTITION_TYPE = MTA_PT2PT_PRE_RECV()

    PARTITION_TYPE = MTA_ETA() # Blocking Static B Level
    PARTITION_TYPE = MTA_PT2PT_ARB() # Non-blocking
    #PARTITION_TYPE = MTB_ETA() # Blocking Static T Level
    PARTITION_TYPE = MTB_ETA_ARB() # Non-blocking

    # PARTITION_TYPE = MTA_PT2PT_ARJIT()
    # PARTITION_TYPE = MTA_PT2PT_ETA_BUT_NB()

    PROFILE = 0
    MAKE_PLOTS = 0
    # MAKE_PLOTS = 1
    VISUALIZE_SCHEDULE = 0
    # VISUALIZE_SCHEDULE = 1

    # Run settings:
    NUM_RUNS = 1

    for _ in range(1):
    # for _ in range(1):
        # Create a csdl-like graph to test
        ccl_graph = create_csdl_like_graph(
            comm,
            graph_size = GRAPH_SIZE,
            super_output = SUPER_OUTPUT,
            load_graph= LOAD_GRAPH,
            # structure = 'embarassing',
            num_paths= 2,
        )

        assign_costs(
            ccl_graph,
            communication_cost = COMM_COST,
            operation_cost = OP_COST,
        )

        # Create a schedule from a choice of algorithms
        scheduler = Scheduler(PARTITION_TYPE, comm)
        schedule = scheduler.schedule(
            ccl_graph,
            profile = PROFILE,
            create_plots = MAKE_PLOTS,
            visualize_schedule = VISUALIZE_SCHEDULE,
        )
        # exit()
        code_object, num_coms, num_ops, num_coms_pre = code_generator(
            schedule,
            ccl_graph,
            VARIABLE_SIZE,
            rank,
            save = 1,
            )
        if rank == 0:
            s = time.time()
        import cProfile
        profiler = cProfile.Profile()
        profiler.enable()
        run_code(code_object, ccl_graph, rank, NUM_RUNS, VARIABLE_SIZE, num_coms, num_ops, num_coms_pre)
        profiler.disable()
        profiler.dump_stats(f'output_{comm.rank}')

        if rank == 0:
            print('\nFINAL TIME:', time.time() - s)