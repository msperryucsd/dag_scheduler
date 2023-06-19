def assign_costs(
        ccl_graph,
        communication_cost,
        operation_cost,
    ):

    for node in ccl_graph.nodes:
        if ccl_graph.nodes[node]['type'] == 'variable':
            ccl_graph.nodes[node]['cost'] = communication_cost
        elif ccl_graph.nodes[node]['type'] == 'operation':
            ccl_graph.nodes[node]['cost'] = operation_cost
            ccl_graph.nodes[node]['time_cost'] = operation_cost
        else:
            raise KeyError('EKJRNEKN')

def normalize_costs(
        ccl_graph,
        max_time, # in seconds
        communication_cost,
    ):
    """
    normalizes time for each operation so that the total time does not exceed max_time
    """

    # Calculate total time
    total_time = 0.0
    for node in ccl_graph.nodes:
        if ccl_graph.nodes[node]['type'] == 'operation':
            total_time += ccl_graph.nodes[node]['time_cost']
    normailization_factor = max_time/total_time

    # assign costs
    for node in ccl_graph.nodes:
        if ccl_graph.nodes[node]['type'] == 'variable':
            ccl_graph.nodes[node]['cost'] = communication_cost
        elif ccl_graph.nodes[node]['type'] == 'operation':
            ccl_graph.nodes[node]['cost'] = ccl_graph.nodes[node]['time_cost']*normailization_factor
            ccl_graph.nodes[node]['time_cost'] = ccl_graph.nodes[node]['time_cost']*normailization_factor
        else:
            raise KeyError('EKJRNEKN')