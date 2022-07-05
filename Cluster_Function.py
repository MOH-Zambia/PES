import networkx as nx
import pandas as pd

# assuming networkx > 2.0 is installed and we have a linked data file 'df' with unique id cols 'id1' and 'id2'

# Cluster Number function
def cluster_number(df, id_1, id_2):

  # Columns
  df = df[['id1', 'id2']]

  # Index column for graphs
  df['index'] = df.index
  
  # Create graph & connected components
  G = nx.from_pandas_edgelist(df, id_1, id_2, 'index')
  # components = nx.connected_component_subgraphs(G)
  components = [G.subgraph(c) for c in nx.connected_components(G)]
  
  # List to append results to
  result = []

  # Loop over components, giving each cluster a cluster number
  for i, cc in enumerate(components, 1):
      idx = [dct['index'] for node1, node2, dct in cc.edges(data=True)]
      group = df.iloc[idx]
      group['Cluster_Number'] = i
      result.append(group)
    
  # Convert result to pandas dataframe then spark dataframe
  result = result.reset_index(drop = True)[[id_1, id_2, 'Cluster_Number']]
  
  # Join cluster number to full data  
  df = pd.merge(df, result, how = 'left', on = ['id1', 'id2'])
  
  # Return spark df
  return df
