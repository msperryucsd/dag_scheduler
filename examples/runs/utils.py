import networkx as nx
from dag_parallelizer import create_csdl_like_graph, assign_costs, Scheduler, normalize_costs

def longest_path_length(G, normalized = False, communication_costs = False):

    if normalized:
        normalize_costs(G, normalized, communication_costs)

    # Create a topological ordering of the graph
    # topological_order = nx.topological_sort(G)

    # Initialize a dictionary to store the longest path length for each node
    longest_path_lengths = {node: 0 for node in G.nodes}
    max_path_length = 0.0

    total_time = 0.0
    # Iterate over the nodes in the topological order
    for node in nx.topological_sort(G):

        # Get the outgoing edges from the current node
        # outgoing_edges = G.out_edges(node)

        if 'time_cost' in G.nodes[node]:
            total_time += G.nodes[node]['time_cost']

        # Update the longest path length for each destination node
        for dest_node in G.successors(node):
            # Calculate the new path length for the destination node

            if 'time_cost' in G.nodes[dest_node]:
                new_path_length = longest_path_lengths[node] + G.nodes[dest_node]['time_cost']
            else:
                new_path_length = longest_path_lengths[node] + 0.0
            # Update the longest path length if the new path is longer
            if new_path_length > longest_path_lengths[dest_node]:
                longest_path_lengths[dest_node] = new_path_length
            max_path_length = max(max_path_length, new_path_length)
    # Find the maximum path length among all nodes
    # max_path_length = max(longest_path_lengths.values())

    return max_path_length, total_time