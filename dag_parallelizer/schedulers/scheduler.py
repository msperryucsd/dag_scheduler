import matplotlib.pyplot as plt

class Scheduler():
    
    def __init__(self, algorithm, comm) -> None:
        self.algorithm = algorithm
        self.algorithm.NUM_RANKS = comm.size
        self.comm = comm

    def schedule(
            self,
            ccl_graph,
            profile = 0,
            create_plots = 0,
            visualize_schedule = 0
        ):
        

        if self.comm.rank == 0:
            self.algorithm.set_create_plots(create_plots)

            if profile:
                import cProfile
                profiler = cProfile.Profile()
                profiler.enable()

            schedule, estimated_timeline = self.algorithm.schedule(ccl_graph)
            check_schedule(schedule, estimated_timeline ,ccl_graph)
            theoretical_makespan = max([x[-1] for x in estimated_timeline])
            print(f'MAKESPAN ESTIMATION: {theoretical_makespan}')
            if visualize_schedule:
                generate_schedule_plot(estimated_timeline, schedule)
                
            if profile:
                profiler.disable()
                profiler.dump_stats('output')
        else:
            schedule = None
        schedule = self.comm.scatter(schedule, root = 0)
        return schedule
    
def generate_schedule_plot(estimated_times, full_schedule):
    fig, ax = plt.subplots(figsize = (4.5,6))
    colormap = {
        'comm':{ 
            'colors': ['chocolate', 'saddlebrown'],
            'alpha': 1,
            'index': 0
            },
        'wait': {
            'colors': ['white'], 
            'alpha': 0,
            'index': 0
            },
        'operation': {
            'colors': ['lightseagreen', 'mediumturquoise'],
            'alpha': 1,
            'index': 0
            },
    }

    num_ranks = len(estimated_times)
    for rank in range(len(estimated_times)):
        values = estimated_times[rank]
        schedule = full_schedule[rank]
        # Calculate the differences between consecutive values
        heights = [values[i+1] - values[i] for i in range(len(values)-1)]
        bottoms = [values[i] for i in range(len(values)-1)]

        # Initialize the bottom of the bars to be zero
        # bottoms = [0] * len(values)

        # Draw each stack of bars
        for i in range(len(heights)):
            height = heights[i]
            # print(height)
            
            if ('GET' in schedule[i]):
                operation_type = 'comm'
            elif ('SEND' in schedule[i]):
                operation_type = 'comm'
                arrow_y = bottoms[i] + (height)/2
                split_string = schedule[i].split("/")
                target_rank = int(split_string[3])

                if target_rank > rank:
                    x_start = rank + 0.45
                    x_end = target_rank - 0.45
                else:
                    x_start = rank - 0.45
                    x_end = target_rank + 0.45

                ax.arrow(
                    x_start,
                    arrow_y,
                    x_end - x_start,
                    0,
                    width = height/15,
                    head_width = height/3,
                    head_length = 0.03,
                    color = 'black',
                    length_includes_head = True,
                )
            elif ('IRECV' in schedule[i]) or  ('SsENDONLY' in schedule[i]) or ('irecvwait' in schedule[i]):
                operation_type = 'comm'
            elif ('WAIT' in schedule[i]) or ('W/F' in schedule[i]):
                operation_type = 'wait'
            else:
                operation_type = 'operation'
            # print(schedule[i], height)
            alpha = colormap[operation_type]['alpha']
            color_list = colormap[operation_type]['colors']
            color_ind = colormap[operation_type]['index']%len(color_list)
            colormap[operation_type]['index']+=1
            ax.bar(rank, height, bottom=bottoms[i], color=color_list[color_ind], alpha = alpha)

        # ax.set_ylim(top = 0.17)
        # ax.set_xlim([-0.75, 7.75])
        ax.set_title(f'Estimated timeline ({num_ranks} processor(s))')
        ax.set_xlabel(f'Processor Index (Rank)')
        ax.set_ylabel(f'Point in Time')
        import matplotlib.patches as mpatches
        red_patch = mpatches.Patch(color='lightseagreen', label='Running Operation')
        red_patch2 = mpatches.Patch(color='chocolate', label='Running MPI Send')
        plt.legend(handles=[red_patch, red_patch2])

            # bottoms[i+1:] = [b+height for b in bottoms[i+1:]]
        # exit('ksjdndkg')
    # Set the x-axis ticks and labels
    # ax.set_xticks([])
    # ax.set_xticklabels([f'{v:.2f}' for v in values])

    plt.savefig(f'schedule_{num_ranks}.png', dpi = 300)
    plt.savefig(f'schedule_{num_ranks}.pdf', dpi = 300)
    # exit()
    # plt.show()

def check_schedule(all_schedules, estimated_timeline, sdag):

    operations = set()
    for node in sdag.nodes:
        if sdag.nodes[node]['type'] == 'operation':
            operations.add(node)

    for rank in range(len(all_schedules)):
        schedule = all_schedules[rank]
        schedule_timeline = estimated_timeline[rank]
        if len(schedule) != len(schedule_timeline)-1:
            raise ValueError('Schedule timeline inconsistent with schedule')
        # print(f'RANK {rank}:')
        # print(f'\t length: {len(schedule)}')
        # print(f'\t length: {len(schedule)}')

        for operation in schedule:
            if ('SsEND' in operation) or ('SEND' in operation) or ('GET' in operation) or ('WAITING' in operation) or ('IRECV' in operation) or ('W/F' in operation) or ('irecvwait' in operation):
                continue
            
            if operation not in sdag.nodes:
                raise KeyError(f'operation {operation} not in DAG.')
            
            operations.remove(operation)
        
    num_ops_left = len(operations)
    if num_ops_left != 0:
        print(operations)
        raise ValueError(f'schedule does not have all operations ( {num_ops_left} left)')
    # exit()