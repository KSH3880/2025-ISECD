import pandas as pd
import networkx as nx
import heapq
import os
import random


def build_graph(config):
    df = pd.read_csv(config["csv_path"], dtype={'u': str, 'v': str})
    df['u'], df['v'] = df['u'].str.strip(), df['v'].str.strip()

    df_u = df[['u', 'u_x', 'u_y']].rename(columns={'u': 'node', 'u_x': 'x', 'u_y': 'y'})
    df_v = df[['v', 'v_x', 'v_y']].rename(columns={'v': 'node', 'v_x': 'x', 'v_y': 'y'})
    nodes = pd.concat([df_u, df_v]).drop_duplicates('node')
    node_pos = {r['node']: (r['x'], r['y']) for _, r in nodes.iterrows()}

    G = nx.DiGraph()
    G.add_nodes_from(node_pos.keys())

    for _, row in df.iterrows():
        dur = row['duration']
        if dur > 400: factor = 0.8
        elif dur > 300: factor = 0.1
        elif dur > 200: factor = 0.2
        elif dur > 100: factor = 0.3
        elif dur > 40:  factor = 0.4
        else:           factor = 0.5
        G.add_edge(row['u'], row['v'], weight=dur * factor)

    return G, df, node_pos

def bidirectional_dijkstra(G, source, target, max_time_diff=60, weight_factor=0.692):
    if source == target:
        return [source], 0, source, [source], [source]

    distF, distB = {source: 0}, {target: 0}
    prevF, prevB = {source: None}, {target: None}
    pqF, pqB = [(0, source)], [(0, target)]
    processedF, processedB = set(), set()
    best_cost, meeting_node = float('inf'), None
    forward_turn = True

    while pqF and pqB:
        if forward_turn:
            curr_dist, u = heapq.heappop(pqF)
            if u in processedF: continue
            processedF.add(u)

            if u in distB:
                total_time = max(distF[u], distB[u])
                if abs(distF[u] - distB[u]) < max_time_diff and total_time < best_cost:
                    best_cost, meeting_node = total_time, u

            for v in G.successors(u):
                w = G[u][v].get('weight', 1) * weight_factor
                new_dist = distF[u] + w
                if new_dist < distF.get(v, float('inf')):
                    distF[v], prevF[v] = new_dist, u
                    heapq.heappush(pqF, (new_dist, v))
        else:
            curr_dist, u = heapq.heappop(pqB)
            if u in processedB: continue
            processedB.add(u)

            if u in distF:
                total_time = max(distF[u], distB[u])
                if abs(distF[u] - distB[u]) < max_time_diff and total_time < best_cost:
                    best_cost, meeting_node = total_time, u

            for v in G.predecessors(u):
                w = G[v][u].get('weight', 1)
                new_dist = distB[u] + w
                if new_dist < distB.get(v, float('inf')):
                    distB[v], prevB[v] = new_dist, u
                    heapq.heappush(pqB, (new_dist, v))

        if best_cost < float('inf'):
            f_min = pqF[0][0] if pqF else float('inf')
            b_min = pqB[0][0] if pqB else float('inf')
            if f_min + b_min >= best_cost:
                break

        forward_turn = not forward_turn

    if meeting_node is None:
        return None, float('inf'), None, [], []

    def reconstruct_path(prev, node):
        path = []
        while node:
            path.append(node)
            node = prev[node]
        return path  # ← 역순 안 함: meeting → source 순서

    f_path = []
    node = meeting_node
    while node is not None:
        f_path.append(node)
        node = prevF[node]
    f_path.reverse()  # source → meeting 순서

    b_path = reconstruct_path(prevB, meeting_node)  # meeting → source 순서 유지

    return f_path + b_path[1:], best_cost, meeting_node, f_path, b_path

def simulate(config, G, df, node_pos):
    random.seed(config["seed"])
    results, all_trials = [], []
    goals = df['v'].unique().tolist()

    for trial in range(1, config["num_trials"] + 1):
        goal = random.choice(goals)
        trial_res = []

        for name, start in config["station_nodes"].items():
            path, cost, meeting, pf, pb = bidirectional_dijkstra(
                G, start, goal, config["max_time_diff"], config["weight_factor"]
            )
            if meeting is None:
                continue

            ft = sum(G[pf[i]][pf[i+1]]['weight'] * config["weight_factor"] for i in range(len(pf) - 1))
            bt = sum(G[pb[i]][pb[i+1]]['weight'] for i in range(len(pb) - 1))  # 원래 코드와 동일
            total = max(ft, bt)

            result = {
                'trial': trial, 'station': name, 'u': start, 'v': goal, 'meeting_node': meeting,
                'u_x': node_pos[start][0], 'u_y': node_pos[start][1],
                'v_x': node_pos[goal][0], 'v_y': node_pos[goal][1],
                'forward_time': round(ft, 2), 'backward_time': round(bt, 2),
                'total_cost': round(total, 2), 'total_time_min': round(total / 60, 2),
                'path_length': len(pf) + len(pb) - 1,
                'forward_path': " → ".join(pf), 'backward_path': " → ".join(pb)
            }
            trial_res.append(result)
            all_trials.append(result)

        if trial_res:
            best = min(trial_res, key=lambda x: x['total_cost'])
            results.append(best)
            print(f"[{trial}] {best['station']} (최소 총비용: {best['total_cost']}, 시간(분): {best['total_time_min']})")

    return pd.DataFrame(results), pd.DataFrame(all_trials)

def save_results(df_all, df_cand, config):
    os.makedirs(os.path.dirname(config["output_path"]), exist_ok=True)
    if not df_cand.empty:
        avg = df_cand[['forward_time', 'backward_time', 'total_cost', 'total_time_min', 'path_length']].mean()
        print("\n=== 전체 평균 ===")
        for k, label in zip(['forward_time', 'backward_time', 'total_cost', 'total_time_min', 'path_length'],
                            ['Forward Time', 'Backward Time', '총비용', '총시간(분)', '경로노드수']):
            print(f"평균 {label}: {avg[k]:.2f}")

        df_all.loc[len(df_all)] = {
            'trial': 'average',
            'forward_time': round(avg['forward_time'], 2),
            'backward_time': round(avg['backward_time'], 2),
            'total_cost': round(avg['total_cost'], 2),
            'total_time_min': round(avg['total_time_min'], 2),
            'path_length': round(avg['path_length'], 2),
            **{k: None for k in df_all.columns if k not in {
                'trial', 'forward_time', 'backward_time', 'total_cost', 'total_time_min', 'path_length'}}
        }

    df_all.to_csv(config["output_path"], index=False, encoding="utf-8-sig")
    print(f"\n✅ 결과 저장 완료: {config['output_path']}")

def init_config():
    return {
        "csv_path": "../data/05280800_assigned_edges_future.csv",
        "output_path": "../data/50times_gowork_bestonly.csv",
        "num_trials": 50,
        "max_time_diff": 60,
        "weight_factor": 0.692,
        "seed": 42,
        "station_nodes": {
            "서대문소방서 북아현119안전센터": "7257925078",
            "마포소방서 119구조대": "8477574118",
            "공덕119안전센터": "2330782485",
            "서대문소방서119구조대": "3825310652"
        }
    }
    
def main():
    config = init_config()
    G, df, node_pos = build_graph(config)
    df_best, df_all = simulate(config, G, df, node_pos)
    save_results(df_best, df_all, config)

if __name__ == "__main__":
    main()
