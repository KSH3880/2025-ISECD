import osmnx as ox
import pandas as pd
from shapely.geometry import Point
from shapely.ops import transform
import pyproj

# ==========================
# 1. 도로망 불러오기 및 노드 병합
# ==========================
def load_graph_with_coords(place_name="Seoul, South Korea"):
    G = ox.graph_from_place(place_name, network_type='drive')
    nodes, edges = ox.graph_to_gdfs(G)

    edges = edges.drop(columns=['u', 'v'], errors='ignore').reset_index()
    u_coords = nodes[['x', 'y']].rename(columns={'x': 'u_x', 'y': 'u_y'})
    v_coords = nodes[['x', 'y']].rename(columns={'x': 'v_x', 'y': 'v_y'})

    edges = edges.merge(u_coords, left_on='u', right_index=True, how='left')
    edges = edges.merge(v_coords, left_on='v', right_index=True, how='left')

    edges['u_point'] = edges.apply(lambda r: Point(r['u_x'], r['u_y']), axis=1)
    edges['v_point'] = edges.apply(lambda r: Point(r['v_x'], r['v_y']), axis=1)

    return edges


# ==========================
# 2. 버퍼 필터링
# ==========================
def filter_by_buffer(edges, station_coords, radius_m=2130):
    to_utm = pyproj.Transformer.from_crs("EPSG:4326", "EPSG:32652", always_xy=True).transform
    to_wgs = pyproj.Transformer.from_crs("EPSG:32652", "EPSG:4326", always_xy=True).transform

    buffers = []
    for lat, lon in station_coords.values():
        buffer = transform(to_utm, Point(lon, lat)).buffer(radius_m)
        buffers.append(transform(to_wgs, buffer))

    def in_all_buffers(row):
        return any(b.contains(row['u_point']) for b in buffers) and \
               any(b.contains(row['v_point']) for b in buffers)

    return edges[edges.apply(in_all_buffers, axis=1)].copy()


# ==========================
# 3. 사각형 필터링
# ==========================
def filter_by_rectangle(edges, min_lon, max_lon, min_lat, max_lat):
    return edges[
        (edges['u_x'] >= min_lon) & (edges['u_x'] <= max_lon) &
        (edges['v_x'] >= min_lon) & (edges['v_x'] <= max_lon) &
        (edges['u_y'] >= min_lat) & (edges['u_y'] <= max_lat) &
        (edges['v_y'] >= min_lat) & (edges['v_y'] <= max_lat)
    ].copy()


# ==========================
# 4. 양방향 엣지 복제하여 단방향화
# ==========================
def duplicate_bidirectional_edges(edges):
    edges = edges.reset_index(drop=True)
    edges['ID'] = edges.index  # ID 부여

    bidir = edges[edges['oneway'] == False].copy()

    for col1, col2 in [('u', 'v'), ('u_x', 'v_x'), ('u_y', 'v_y'), ('u_point', 'v_point')]:
        bidir[col1], bidir[col2] = bidir[col2], bidir[col1]

    bidir['ID'] = range(edges['ID'].max() + 1, edges['ID'].max() + 1 + len(bidir))
    bidir['key'] = 0  # 필요시 고정값 사용

    augmented = pd.concat([edges, bidir], ignore_index=True)

    # ID 열을 맨 앞으로 이동
    cols = ['ID'] + [col for col in augmented.columns if col != 'ID']
    return augmented[cols]



# ==========================
# 5. 실행
# ==========================
if __name__ == "__main__":
    # 1. 도로 및 노드 좌표 포함 엣지 로드
    edges = load_graph_with_coords()

    # 2. 정류장 기준 버퍼 필터링
    station_coords = {
        "합정역": (37.5499, 126.9136),
        "홍대입구역": (37.5563, 126.9220),
        "신촌역": (37.5551, 126.9368),
        "아현역": (37.5566, 126.9563),
        "서대문역": (37.5650, 126.9667),
        "세종로": (37.5727, 126.9769),
        "종로3가": (37.5705, 126.9918),
        "동대문역": (37.5715, 127.0110)
    }
    edges = filter_by_buffer(edges, station_coords)

    # 3. 직사각형 필터링
    edges = filter_by_rectangle(
        edges,
        min_lon=126.9284283, max_lon=126.9691612,
        min_lat=37.5449817, max_lat=37.5683276
    )

    # 4. 양방향 엣지를 단방향으로 확장
    final_edges = duplicate_bidirectional_edges(edges)

    # 5. 저장
    final_edges.to_csv("final_augmented_edges.csv", index=False, encoding='utf-8-sig')
    print(f"✅ 저장 완료: final_augmented_edges.csv / 총 {len(final_edges)}개 엣지")
