import pandas as pd
import csv

base_dir = '/home/timothy/Projects/assignments/tia_text_analytics/data'

posts_csv = base_dir + '/staging/posts.csv'
companies_csv = base_dir + '/staging/posts_companies.csv'
comments_csv = base_dir + '/staging/comments_dedup.csv'

companies_node_csv = base_dir + '/network_input/companies_node.csv'
authors_node_csv = base_dir + '/network_input/authors_node.csv'
all_edges_csv = base_dir + '/network_input/edges.csv'

posts_df = pd.read_csv(posts_csv)
companies_df = pd.read_csv(companies_csv)
comments_df = pd.read_csv(comments_csv)



# rename columns
posts_df = posts_df.rename(index=str, columns={'id': 'post_id'})
companies_df = companies_df.rename(index=str, columns={'id': 'company_id'})
comments_df = comments_df.rename(index=str, columns={'id': 'comment_id', 'post': 'post_id', 'parent':'parent_id'})

# subset columns
posts_df = posts_df[['post_id', 'date_gmt', 'comments_count', 'is_sponsored', 'title', 'read_time', 'type',
                     'author_id', 'author_display_name', 'author_roles']]
companies_df = companies_df[['company_id', 'name', 'post_id', 'date_founded']]
comments_df = comments_df[['comment_id', 'date_gmt', 'post_id', 'excerpt', 'parent_id',
                           'author_id', 'author_display_name', 'author_roles']]


# nodes (company, authors)
# edges (posts, comments)

# prepare node data for authors
author_posts_df = posts_df[['author_id', 'author_display_name', 'author_roles']]
author_comments_df = comments_df[['author_id', 'author_display_name', 'author_roles']]
authors_node_df = pd.concat([author_posts_df, author_comments_df])
authors_node_df = authors_node_df.drop_duplicates(keep='first')
authors_node_df = authors_node_df.rename(index=str, columns={'author_id': 'id', 'author_display_name': 'label'})
authors_node_df['type'] = 'author'

# prepare node data for companies
companies_node_df = companies_df.drop_duplicates(subset=['company_id', 'name', 'date_founded'], keep='first')
companies_node_df = companies_node_df[['company_id', 'name', 'date_founded']]
companies_node_df['type'] = 'company'
companies_node_df = companies_node_df.rename(index=str, columns={'company_id': 'id', 'name':'label'})

authors_node_df.index.name = 'index_id'
companies_node_df.index.name = 'index_id'
authors_node_df.to_csv(authors_node_csv, quoting=csv.QUOTE_ALL)
companies_node_df.to_csv(companies_node_csv, quoting=csv.QUOTE_ALL)


# edges :
#   author -> company (through post)
#   author -> company (through comments)
#   author -> author (through comments)
# print(posts_df.shape)
author_to_posts_df = pd.merge(posts_df, companies_df, on='post_id')
author_to_posts_df = author_to_posts_df[['post_id', 'company_id', 'author_id', 'title']]
# print(author_to_posts_df.shape)

# prepare edge data: author -> company (through posts)
author_to_company_df = author_to_posts_df[['company_id', 'author_id', 'title']]
author_to_company_df = author_to_company_df.rename(index=str, columns={'author_id': 'source', 'company_id': 'target', 'title': 'excerpt'})
author_to_company_df['source_type'] = 'author'
author_to_company_df['target_type'] = 'company'
author_to_company_df['edge_type'] = 'post'
# # Note, not all posts are linked to companies, hence there will be less records here than there are posts
# print(author_to_company_df.shape)
# print(author_to_company_df)


# prepare edge data: author -> company (through comments)
comments_to_posts_df = pd.merge(author_to_posts_df[['post_id', 'company_id']], comments_df, on='post_id')
comments_to_posts_df = comments_to_posts_df[['post_id', 'company_id', 'comment_id', 'parent_id', 'author_id', 'excerpt']]
# print(comments_to_posts_df)
# print(comments_to_posts_df.shape)

# if parent_id = 0, then its a comment on the post/company directly
comments_on_posts_df = comments_to_posts_df.loc[comments_to_posts_df['parent_id'] == 0]
comments_on_posts_df = comments_on_posts_df[['company_id', 'author_id', 'excerpt']]
comments_on_posts_df = comments_on_posts_df.rename(index=str, columns={'author_id': 'source', 'company_id': 'target'})
comments_on_posts_df['source_type'] = 'author'
comments_on_posts_df['target_type'] = 'company'
comments_on_posts_df['edge_type'] = 'comment'
# print(comments_on_posts_df.shape)
# Note, the above is limited by comments on posts with companies

# prepare edge data: author -> author (through comments)
# join back with itself to join the author_id of the parent post
comment_parents_df = comments_df[['comment_id','author_id']]
comment_parents_df = comment_parents_df.rename(index=str, columns={'comment_id':'parent_id', 'author_id': 'parent_author_id'})
comments_on_comments_df = pd.merge(comments_df, comment_parents_df, on='parent_id')
# print(comments_on_comments_df)

# if parent_id != 0, then we use the parent_id instead of post_id
comments_on_comments_df = comments_on_comments_df.loc[comments_on_comments_df['parent_id'] != 0]
comments_on_comments_df = comments_on_comments_df[['author_id', 'parent_author_id', 'excerpt']]
comments_on_comments_df = comments_on_comments_df.rename(index=str, columns={'author_id': 'source', 'parent_author_id': 'target'})
comments_on_comments_df['source_type'] = 'author'
comments_on_comments_df['target_type'] = 'author'
comments_on_comments_df['edge_type'] = 'comment'
# print(comments_on_comments_df.shape)
# print(comments_on_comments_df)


all_edges_df = pd.concat([author_to_company_df, comments_on_posts_df, comments_on_comments_df])
all_edges_df = all_edges_df.rename(index=str, columns={'excerpt': 'label'})
all_edges_df.index.name = 'index_id'

# print(all_edges_df)
all_edges_df.to_csv(all_edges_csv, quoting=csv.QUOTE_ALL)


# instead of using companies, we can use posts instead
