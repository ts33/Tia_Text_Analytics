import networkx as nx
import pandas as pd
import csv


base_dir = '/home/timothy/Projects/assignments/tia_text_analytics/data'

companies_node_csv = base_dir + '/network_input/companies_node.csv'
authors_node_csv = base_dir + '/network_input/authors_node.csv'
all_edges_csv = base_dir + '/network_input/edges.csv'
centrality_csv = base_dir + '/network_output/centralities.csv'

companies_node_df = pd.read_csv(companies_node_csv, quoting=csv.QUOTE_ALL)
authors_node_df = pd.read_csv(authors_node_csv, quoting=csv.QUOTE_ALL)
edges_df = pd.read_csv(all_edges_csv, quoting=csv.QUOTE_ALL)



DG = nx.DiGraph()


for index,row in edges_df.iterrows():  # edge_type|excerpt|from|from_type|to|to_type
    from_node, to_node = str(row['source']), str(row['target'])
    # edge_data = {
    #     'source_type': row['source_type'],
    #     'target_type': row['target_type'],
    #     'edge_type':   row['edge_type'],
    #     'label':       row['label']
    # }
    DG.add_edges_from([(from_node, to_node)])



in_degree = nx.in_degree_centrality(DG)
out_degree = nx.out_degree_centrality(DG)
eigen = nx.eigenvector_centrality_numpy(DG)


def transform_to_series(centrality):
    label = []
    values = []
    for key, value in centrality.items():
        label.append(key)
        values.append(value)

    return {'label': label, 'values': values}


in_degree_series = transform_to_series(in_degree)
out_degree_series = transform_to_series(out_degree)
eigen_series = transform_to_series(eigen)

in_df = pd.DataFrame(in_degree_series)
out_df = pd.DataFrame(out_degree_series)
eigen_df = pd.DataFrame(eigen_series)
print(in_df.shape)
print(out_df.shape)
print(eigen_df.shape)


# in_df = in_df.loc[in_df['values'] != 0]
# out_df = out_df.loc[out_df['values'] != 0]
# eigen_df = eigen_df.loc[eigen_df['values'] != 0]
# print(in_df.shape)
# print(out_df.shape)
# print(eigen_df.shape)


in_df = in_df.rename(index=str, columns={'values': 'in_degree_centrality'})
out_df = out_df.rename(index=str, columns={'values': 'out_degree_centrality'})
eigen_df = eigen_df.rename(index=str, columns={'values': 'eigenvector_centrality'})

in_out_df = pd.merge(in_df, out_df, how='outer', on='label')
all_df = pd.merge(in_out_df, eigen_df, how='outer', on='label')


all_nodes_df = pd.concat([companies_node_df, authors_node_df])
all_nodes_df = all_nodes_df.rename(index=str, columns={'label': 'name', 'id': 'label'})
all_nodes_df['label'] = all_nodes_df['label'].apply(lambda x: str(x))
full_df = pd.merge(all_df, all_nodes_df, on='label')

full_df.to_csv(centrality_csv, quoting=csv.QUOTE_ALL)
