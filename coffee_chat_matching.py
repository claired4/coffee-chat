from ortools.graph import pywrapgraph
import csv

# ----------- ACTION REQ'D: FILL IN BELOW VARIABLES: -------
days = ['M', 'T', 'W', 'T', 'F']
timeslots = ['9:00 AM', '9:30 AM', '10:00 AM', '10:30 AM', '11:00 AM', '11:30 AM',
             '12:00 PM', '12:30 PM', '1:00 PM', '1:30 PM', '2:00 PM', '2:30 PM',
             '3:00 PM', '3:30 PM', '4:00 PM', '4:30 PM', '5:00 PM', '5:30 PM',
             '6:00 PM', '6:30 PM', '7:00 PM', '7:30 PM', '8:00 PM', '8:30 PM',
             '9:00 PM', '9:30 PM', '10:00 PM', '10:30 PM']
sbc_path = 'sbc_coffee_chat_20.csv'
pnm_path = 'pnm_coffee_chat_20.csv'

# ---------------------------------------------------------
#     NO NEED TO CHANGE ANYTHING BELOW THIS POINT

id = 2
id2pnm, pnm2id = {}, {}
id2time, time2id = {}, {}
id2sbc, sbc2id = {}, {}
source, sink = 0, 1
max_flow = pywrapgraph.SimpleMaxFlow()

def assign_ids_to_timeslots():
  global id, id2time, time2id
  for day in days:
    for time in timeslots:
      id2time[id] = (day, time)
      time2id[(day, time)] = id
      id += 1

def process_pnm_csv(path):
  '''
  CSV format:
  timestamp, name, email, day1, day2, day3, day4, day5 ... 
  '''
  global id, id2pnm, pnm2id

  with open(path, mode='r') as pnm_file:
    csv_reader = csv.reader(pnm_file)
    lines = 0
    for row in csv_reader:
      # skip labels
      if lines == 0: 
        lines += 1
        continue

      # assign pnm to an id/node
      name, email = row[1:3]
      if (name, email) in pnm2id:
        continue
      id2pnm[id] = (name, email)
      pnm2id[(name, email)] = id

      # add edge of capacity 1 between source and member
      max_flow.AddArcWithCapacity(source, id, 1)
      
      # add edge of capacity 1 between pnm and available timeslots
      for i in range(len(days)):
        day = days[i]
        times_avail = [t.strip() for t in row[i+3].split(',')]
        for time in times_avail:
          if len(time) > 0:
            max_flow.AddArcWithCapacity(id, time2id[(day, time)], 1)
      
      id += 1
      lines +=1

      # for testing purposes
      # if lines > 1:
      #   break

def process_sbc_csv(path):
  '''
  CSV format:
  timestamp, name, email, chats willing, day1, day2, day3, day4, day5 ... 
  '''
  global id, id2sbc, sbc2id

  with open(path, mode='r') as sbc_file:
    csv_reader = csv.reader(sbc_file)
    lines = 0
    for row in csv_reader:
      # skip labels
      if lines == 0: 
        lines += 1
        continue

      # assign member to an id/node
      name, email, c = row[1:4]
      if (name, email) in sbc2id:
        continue
      id2sbc[id] = (name, email)
      sbc2id[(name, email)] = id
      
      # add edge of capacity 1 between available timeslots and members
      for i in range(len(days)):
        day = days[i]
        times_avail = [t.strip() for t in row[i+4].split(',')]
        for time in times_avail:
          if len(time) > 0:
            max_flow.AddArcWithCapacity(time2id[(day, time)], id, 1)
      
      # add edge of capacity c between member and sink
      max_flow.AddArcWithCapacity(id, sink, int(c))

      id += 1
      lines +=1

      # for testing purposes
      # if lines > 1:
      #   break

def generate_pairings():
  '''
  generate pairings / time slot assignments by recovering saturated edges
  '''
  pnm_assignments, sbc_assignments = {}, {} # time slot --> list of member ids

  for i in range(max_flow.NumArcs()):
    if max_flow.Flow(i) < 1:
      continue
    if max_flow.Tail(i) in id2time:
      # print(str(max_flow.Tail(i)) + " " + str(max_flow.Head(i)))
      time_id = max_flow.Tail(i)
      sbc_assignments[time_id] = sbc_assignments.get(time_id, [])
      sbc_assignments[time_id].append(max_flow.Head(i))
    if max_flow.Head(i) in id2time:
      time_id = max_flow.Head(i)
      pnm_assignments[time_id] = pnm_assignments.get(time_id, [])
      pnm_assignments[time_id].append(max_flow.Tail(i))

  assigned_pnms = set()
  with open('final_assignments.csv', mode='w') as final_assignments:
    final_writer = csv.writer(final_assignments, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
    
    for time_id, pnms in pnm_assignments.items():
      sbcs = sbc_assignments[time_id]
      for i in range(len(pnms)):
        assigned_pnms.add(pnms[i])
        time, sbc, pnm = id2time[time_id], id2sbc[sbcs[i]], id2pnm[pnms[i]]
        final_writer.writerow([time[0], time[1], sbc[0], sbc[1], pnm[0], pnm[1]])

  if len(assigned_pnms) != len(id2pnm):
    print('ASSIGNMENT NOT OPTIMAL, UNASSIGNED PNMS ARE LISTED BELOW:')
    for pnm_id in (set(id2pnm.keys()) - assigned_pnms):
      print(id2pnm[pnm_id])
    return 
  
  print('OPTIMAL ASSIGNMENT FOUND FOR ' + str(len(id2pnm)) + ' PNMS, SEE GENERATED CSV' )

def print_flows():
  print('  Arc    Flow / Capacity')
  for i in range(max_flow.NumArcs()):
    print('%1s -> %1s   %3s  / %3s' % (
        max_flow.Tail(i),
        max_flow.Head(i),
        max_flow.Flow(i),
        max_flow.Capacity(i)))

def example():
  """MaxFlow simple interface example."""

  # Define three parallel arrays: start_nodes, end_nodes, and the capacities
  # between each pair. For instance, the arc from node 0 to node 1 has a
  # capacity of 20.

  start_nodes = [0, 0, 0, 1, 1, 2, 2, 3, 3]
  end_nodes = [1, 2, 3, 2, 4, 3, 4, 2, 4]
  capacities = [20, 30, 10, 40, 30, 10, 20, 5, 20]

  # Instantiate a SimpleMaxFlow solver.
  max_flow = pywrapgraph.SimpleMaxFlow()
  # Add each arc.
  for i in range(0, len(start_nodes)):
    max_flow.AddArcWithCapacity(start_nodes[i], end_nodes[i], capacities[i])

  # Find the maximum flow between node 0 and node 4.
  if max_flow.Solve(0, 4) == max_flow.OPTIMAL:
    print('Max flow:', max_flow.OptimalFlow())
    print('')
    print('  Arc    Flow / Capacity')
    for i in range(max_flow.NumArcs()):
      print('%1s -> %1s   %3s  / %3s' % (
          max_flow.Tail(i),
          max_flow.Head(i),
          max_flow.Flow(i),
          max_flow.Capacity(i)))
    print('Source side min-cut:', max_flow.GetSourceSideMinCut())
    print('Sink side min-cut:', max_flow.GetSinkSideMinCut())
  else:
    print('There was an issue with the max flow input.')


if __name__ == '__main__':
  assign_ids_to_timeslots()
  process_pnm_csv(pnm_path)
  process_sbc_csv(sbc_path)

  max_flow.Solve(source, sink)
  generate_pairings()
  # print(max_flow.OptimalFlow())

  # print_flows()