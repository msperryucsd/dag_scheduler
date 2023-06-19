import matplotlib.pyplot as plt
import seaborn as sns
import networkx as nx
from utils import longest_path_length
from dag_parallelizer.graph_generator.sdag_to_op_only_graph import sdag_to_op_only_graph
sns.set(font_scale=2.5, rc={'text.usetex' : True})
sns.set_style("ticks")

import os
import pickle

# Specify the path to the 'graphs' directory
graphs_dir = "graphs"
graph_properties = {}

# Create an empty master dictionary
master_dict = {}
import cProfile
profiler = cProfile.Profile()
profiler.enable()

# Loop over each directory in 'graphs'
for graph_name in os.listdir(graphs_dir):
    dir_path = os.path.join(graphs_dir, graph_name)
    print('processing:', graph_name)
    if os.path.isdir(dir_path):
        stats_file = os.path.join(dir_path, "stats.pickle")
        
        # Check if 'stats.pickle' exists in the current graph_name
        if os.path.isfile(stats_file):
            # Open and load the 'stats.pickle' file
            with open(stats_file, "rb") as f:
                stats = pickle.load(f)
            # Add the stats to the master dictionary
            master_dict[graph_name] = stats

        graph_file = os.path.join(dir_path, graph_name)
        with open(f'{graph_file}.pickle', 'rb') as f:
            graph = pickle.load(f)
            # G,_ =  sdag_to_op_only_graph(graph)
            # print(G)
            # print(graph)
            # print(stats)
            # for key in stats:
            #     print(key)
            #     for key2 in stats[key]:
            #         print('\t', key2)

            # exit()
            inner_dict = stats[list(stats.keys())[0]]
            one_proc_time = stats[1][10000]
            one_proc_time = one_proc_time[list(one_proc_time.keys())[0]]['final_time']
            inner_dict = inner_dict[list(inner_dict.keys())[0]]
            inner_dict = inner_dict[list(inner_dict.keys())[0]]
            dag_longest_path, total_time = longest_path_length(graph, normalized=inner_dict['max_time'], communication_costs = inner_dict['communication_cost'])
            graph_properties[graph_name] = {}
            graph_properties[graph_name]['longest_path'] = dag_longest_path
            graph_properties[graph_name]['min_time'] = dag_longest_path/inner_dict['max_time']
            graph_properties[graph_name]['one_proc_time'] = one_proc_time
            graph_properties[graph_name]['total_time'] = total_time
            
            graph_builds = graph_name.split('_')
            num_nodes = len(graph.nodes)
            structure = str(graph_builds[1])
            # print(structure)
            variety_id = int(graph_builds[2])
            graph_properties[graph_name]['structure'] = structure
            graph_properties[graph_name]['num_nodes'] = num_nodes
            graph_properties[graph_name]['variety'] = variety_id


profiler.disable()
profiler.dump_stats('output')
# exit()

# Print the master dictionary
# for directory, stats in master_dict.items():
#     print(f"Directory: {directory}")
#     print(f"Stats: {stats}")
#     print("---")



def get_plot_data(filter_input = None, filter = None):
    procs_dict = {}

    filtered_check_dict = {}
    for graph_name in master_dict:
        stats = master_dict[graph_name]
        for num_procs in stats:
            for variable_size in stats[num_procs]:
                for partition_type in stats[num_procs][variable_size]:
                    procs_dict[partition_type] = {}
                    filtered_check_dict[partition_type] = {'filtered':0, 'total':0}
    for graph_name in master_dict:
        stats = master_dict[graph_name]
        for num_procs in stats:
            for variable_size in stats[num_procs]:
                for partition_type in stats[num_procs][variable_size]:

                    if num_procs not in procs_dict[partition_type]:
                        procs_dict[partition_type][num_procs] = {'x': [], 'y': []}

                    filter_checks = {
                        'num_procs': num_procs,
                        'variable_size': variable_size,
                        'partition_type': partition_type,
                        'structure':graph_properties[graph_name]['structure'],
                    }
                    filtered_check_dict[partition_type]['total'] += 1
                    if filter is not None:
                        if not filter(filter_checks[filter_input]):
                            filtered_check_dict[partition_type]['filtered'] += 1
                            continue
                    # print(variable_size)
                    final_time = stats[num_procs][variable_size][partition_type]['final_time']
                    one_proc_time = graph_properties[graph_name]['one_proc_time']
                    #procs_dict[partition_type][num_procs]['y'].append(one_proc_time/final_time)
                    #procs_dict[partition_type][num_procs]['x'].append(graph_properties[graph_name]['min_time'])

                    procs_dict[partition_type][num_procs]['y'].append(final_time/one_proc_time)
                    procs_dict[partition_type][num_procs]['x'].append(graph_properties[graph_name]['longest_path'])
    print(f'building data ({filter_input})')
    for partition_type in filtered_check_dict:
        print(f'\t{partition_type}: {filtered_check_dict[partition_type]["filtered"]}/{filtered_check_dict[partition_type]["total"]}')
    return procs_dict

# exit()
def plot_data(procs_dict, title):
    import numpy as np
    theoretical_x = np.arange(0,1.2,0.1)
    theoretical_y = np.arange(0,1.2,0.1)

    fig, axs = plt.subplots(1, len(procs_dict), figsize=(30, 15))
    fig.suptitle(title)
    for i, partition_type in enumerate(['Static B-Level', 'Static T-Level','Static B-Level (Non-blocking)','Static T-Level (Non-blocking)']):
        sp = axs[i]
        sp.set_ylabel('Measured Time')
        sp.set_xlabel('Theoretical minimum possible time')
        sp.set_ylim([0.1,1.3])
        sp.set_xlim([0.001,1.3])
        sp.set_title(partition_type)
        plot_dict = procs_dict[partition_type]
        for num_procs in plot_dict:
            # print(num_procs)
            # print(procs_dict[num_procs]['x'])
            # exit()
            sp.scatter(plot_dict[num_procs]['x'], plot_dict[num_procs]['y'], label=f'{num_procs} processors', s = 200)
        sp.loglog(theoretical_x, theoretical_y)
        sp.legend()
    plt.tight_layout()
    plt.savefig(f'figs/{title}.pdf', dpi =300)


# All Graphs
procs_dict = get_plot_data()
plot_data(procs_dict, title = 'Every Graph')

# Large communication cost
filter = lambda x: x < 1000
procs_dict = get_plot_data('variable_size', filter)
plot_data(procs_dict, title = 'Graphs with Small Array Communications')

# Large communication cost
filter = lambda x: x > 100
procs_dict = get_plot_data('variable_size', filter)
plot_data(procs_dict, title = 'Graphs with Large Array Communications')

# Embarassingly Parallel
filter = lambda x: x == 'e'
procs_dict = get_plot_data('structure', filter)
plot_data(procs_dict, title = 'Type A Graphs (Embarassingly Parallel)')


# Embarassingly Parallel
filter = lambda x: x == 'ed'
procs_dict = get_plot_data('structure', filter)
plot_data(procs_dict, title = 'Type B Graphs (Mostly Parallel)')

# Embarassingly Parallel
filter = lambda x: x == 'r'
procs_dict = get_plot_data('structure', filter)
plot_data(procs_dict, title = 'Type C Graphs (Random Graphs)')


# Two Processors
filter = lambda x: x == 2
procs_dict = get_plot_data('num_procs', filter)
plot_data(procs_dict, title = 'Two Processors')

# Two Processors
filter = lambda x: x == 9
procs_dict = get_plot_data('num_procs', filter)
plot_data(procs_dict, title = 'Nine Processors')

plt.show()
