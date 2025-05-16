import osmnx as ox

# 서울시 전체 도로망 다운로드
G = ox.graph_from_place("Seoul, South Korea", network_type='drive')

# 노드와 엣지 확인
nodes, edges = ox.graph_to_gdfs(G)

# 기존 u, v 컬럼이 있으면 제거
edges = edges.drop(columns=['u', 'v'], errors='ignore')

# 인덱스를 컬럼으로 만들기
edges_reset = edges.reset_index()

# u와 v 좌표를 가져오기 위한 nodes에서의 좌표 DataFrame
u_coords = nodes[['x', 'y']].rename(columns={'x': 'u_x', 'y': 'u_y'})
v_coords = nodes[['x', 'y']].rename(columns={'x': 'v_x', 'y': 'v_y'})

# u 좌표 merge
edges_reset = edges_reset.merge(u_coords, left_on='u', right_index=True, how='left')

# v 좌표 merge
edges_reset = edges_reset.merge(v_coords, left_on='v', right_index=True, how='left')


# osmid 열을 맨 앞으로 이동
cols = ['osmid'] + [col for col in edges_reset.columns if col != 'osmid']
edges_reset = edges_reset[cols]


####여기 연결 수정 일단 임시로로
# # CSV로 저장
# edges_reset.to_csv('edges_with_coords.csv', index=False, encoding='utf-8-sig')

# # CSV 파일 불러오기
# edges_df = pd.read_csv("filtered_edges.csv")  # 파일 경로를 알맞게 설정

edges_df = edges_reset.copy()   

# oneway=False인 엣지만 선택
bidirectional_edges = edges_df[edges_df['oneway'] == False].copy()

# u, v 및 관련 좌표 정보 뒤집기
bidirectional_edges['u'], bidirectional_edges['v'] = bidirectional_edges['v'], bidirectional_edges['u']
bidirectional_edges['u_x'], bidirectional_edges['v_x'] = bidirectional_edges['v_x'], bidirectional_edges['u_x']
bidirectional_edges['u_y'], bidirectional_edges['v_y'] = bidirectional_edges['v_y'], bidirectional_edges['u_y']
bidirectional_edges['u_point'], bidirectional_edges['v_point'] = bidirectional_edges['v_point'], bidirectional_edges['u_point']

# 새로운 ID 부여
max_id = edges_df['ID'].max()
bidirectional_edges['ID'] = range(max_id + 1, max_id + 1 + len(bidirectional_edges))
bidirectional_edges['key'] = 0  # key는 새로 부여하거나 고정값

# 역방향 edge를 원본 데이터에 추가
augmented_edges_df = pd.concat([edges_df, bidirectional_edges], ignore_index=True)



####여기도 연결 수정 일단 임시로로
# # 결과 저장
# augmented_edges_df.to_csv("augmented_edges.csv", index=False)

# #####csv 추가
# edges = pd.read_csv('augmented_edges.csv')
# df=edges.copy()

df=augmented_edges_df.copy()


# Define rectangle boundaries from the user's coordinates
min_lon = 126.9284283
max_lon = 126.9691612
min_lat = 37.5449817
max_lat = 37.5683276

# Filter rows where both u and v points are inside the rectangle
filtered_df = df[
    (df['u_x'] >= min_lon) & (df['u_x'] <= max_lon) &
    (df['v_x'] >= min_lon) & (df['v_x'] <= max_lon) &
    (df['u_y'] >= min_lat) & (df['u_y'] <= max_lat) &
    (df['v_y'] >= min_lat) & (df['v_y'] <= max_lat)
]