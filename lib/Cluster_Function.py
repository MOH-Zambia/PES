import networkx as nx
import pandas as pd


# Cluster Number function
def cluster_number(df, id_1, id_2):
    # Columns
    df_cluster = df[[id_1, id_2]].copy()

    # Index column for graphs
    df_cluster['index'] = df_cluster.index

    # Create graph & connected components
    G = nx.from_pandas_edgelist(df_cluster, id_1, id_2, 'index')

    # components = nx.connected_component_subgraphs(G)
    components = [G.subgraph(c) for c in nx.connected_components(G)]

    # empty list to append results to
    result = []

    # Loop over components, giving each cluster a cluster number
    for i, cc in enumerate(components, 1):
        idx = [dct['index'] for node1, node2, dct in cc.edges(data=True)]
        group = df_cluster.loc[idx]
        group['Cluster_Number'] = i
        result.append(group)

    # Convert result to pandas dataframe
    if len(result)>1:
        result = pd.concat(result).reset_index(drop=True)[[id_1, id_2, 'Cluster_Number']]

        # Join cluster number to full data
        df = pd.merge(df, result, how='left', on=[id_1, id_2])

    else:
        print("No clusters to merge")
        df["Cluster_Number"] = None

    return df

# output = cluster_number(df = df, id_1 = '', id_2 = '')
