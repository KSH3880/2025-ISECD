import pandas as pd
import folium

# 1. CSV 불러오기
edges = pd.read_csv("final_augmented_edges.csv")

# 2. 지도 중심 좌표 계산
center_lat = (edges['u_y'].mean() + edges['v_y'].mean()) / 2
center_lon = (edges['u_x'].mean() + edges['v_x'].mean()) / 2

# 3. folium 지도 생성
m = folium.Map(location=[center_lat, center_lon], zoom_start=14)

# 4. 엣지를 folium PolyLine으로 시각화
for _, row in edges.iterrows():
    line = [(row['u_y'], row['u_x']), (row['v_y'], row['v_x'])]  # (위도, 경도)
    folium.PolyLine(locations=line, color="blue", weight=2, opacity=0.6).add_to(m)

# 5. HTML로 저장 또는 Jupyter에서 바로 출력
m.save("edge_map.html")
m  # Jupyter에서는 이 줄로 출력됨
