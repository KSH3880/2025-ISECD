import pandas as pd
import networkx as nx
import heapq
import os
import random

def build_graph(config):
    df = pd.read_csv(config["csv_path"], dtype={'u': str, 'v': str})
    df['u'] = df['u'].str.strip()
    df['v'] = df['v'].str.strip()

    df_u = df[['u', 'u_x', 'u_y']].rename(columns={'u': 'node', 'u_x': 'x', 'u_y': 'y'})
    df_v = df[['v', 'v_x', 'v_y']].rename(columns={'v': 'node', 'v_x': 'x', 'v_y': 'y'})
    nodes = pd.concat([df_u, df_v]).drop_duplicates('node')
    node_pos = dict(zip(nodes['node'], zip(nodes['x'], nodes['y'])))

    G = nx.DiGraph()
    G.add_nodes_from(node_pos)

    def compute_factor(duration):
        if duration > 400: return 0.8
        elif duration > 300: return 0.1
        elif duration > 200: return 0.2
        elif duration > 100: return 0.3
        elif duration > 40: return 0.4
        return 0.5

    for _, row in df.iterrows():
        weight = row['duration'] * compute_factor(row['duration'])
        G.add_edge(row['u'], row['v'], weight=weight)

    return G, df, node_pos

def reconstruct_path(prev, node):
    path = []
    while node is not None:
        path.append(node)
        node = prev[node]
    return path[::-1]

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
        pq, dist, prev, processed, other_dist, forward = (
            (pqF, distF, prevF, processedF, distB, True)
            if forward_turn else
            (pqB, distB, prevB, processedB, distF, False)
        )
        if not pq:
            break
        curr_dist, u = heapq.heappop(pq)
        if u in processed:
            continue
        processed.add(u)

        if u in other_dist:
            total_time = max(distF.get(u, float('inf')), distB.get(u, float('inf')))
            if abs(distF.get(u, 0) - distB.get(u, 0)) < max_time_diff and total_time < best_cost:
                best_cost, meeting_node = total_time, u

        neighbors = G.successors(u) if forward else G.predecessors(u)
        for v in neighbors:
            edge_weight = G[u][v].get('weight', 1) * weight_factor if forward else G[v][u].get('weight', 1)
            new_dist = dist[u] + edge_weight
            if new_dist < dist.get(v, float('inf')):
                dist[v], prev[v] = new_dist, u
                heapq.heappush(pq, (new_dist, v))

        if best_cost < float('inf'):
            f_min = pqF[0][0] if pqF else float('inf')
            b_min = pqB[0][0] if pqB else float('inf')
            if f_min + b_min >= best_cost:
                break

        forward_turn = not forward_turn

    if meeting_node is None:
        return None, float('inf'), None, [], []

    f_path = reconstruct_path(prevF, meeting_node)
    b_path = reconstruct_path(prevB, meeting_node)
    return f_path + b_path[1:], best_cost, meeting_node, f_path, b_path

def load_risk_map(edge_csv_path):
    edge_df = pd.read_csv(edge_csv_path, dtype={'u': str, 'v': str})
    edge_df['u'] = edge_df['u'].str.strip()
    edge_df['v'] = edge_df['v'].str.strip()
    if 'risk' not in edge_df.columns:
        edge_df['risk'] = edge_df['length'] * 0.1
    return {(row['u'], row['v']): row['risk'] for _, row in edge_df.iterrows()}

def compute_total_risk_from_path_str(forward_str, edge_risk_map):
    if not isinstance(forward_str, str): return 0.0
    nodes = [n.strip() for n in forward_str.split("→") if n.strip()]
    return sum(edge_risk_map.get((nodes[i], nodes[i + 1]), edge_risk_map.get((nodes[i + 1], nodes[i]), 0))
               for i in range(len(nodes) - 1))

def simulate(config, G, df, node_pos, risk_map):
    random.seed(config["seed"])
    results = []
    goals = config["accident_candidates"]

    for trial in range(1, config["num_trials"] + 1):
        goal = random.choice(goals)
        trial_res = []

        for name, start in config["station_nodes"].items():
            path, cost, meeting, pf, pb = bidirectional_dijkstra(
                G, start, goal, config["max_time_diff"], config["weight_factor"]
            )
            if meeting is None:
                continue

            ft = sum(G[pf[i]][pf[i + 1]]['weight'] * config["weight_factor"] for i in range(len(pf) - 1))
            bt = sum(G[pb[i]].get(pb[i + 1], {}).get('weight', 0) for i in range(len(pb) - 1))
            total = max(ft, bt)
            forward_path_str = " → ".join(pf)

            total_risk = compute_total_risk_from_path_str(forward_path_str, risk_map)

            alpha = config.get("alpha", 0.7)
            total_score = alpha * total + (1 - alpha) * total_risk

            result = {
                'trial': trial, 'station': name, 'u': start, 'v': goal, 'meeting_node': meeting,
                'u_x': node_pos[start][0], 'u_y': node_pos[start][1],
                'v_x': node_pos[goal][0], 'v_y': node_pos[goal][1],
                'forward_time': round(ft, 2), 'backward_time': round(bt, 2),
                'total_cost': round(total, 2), 'total_risk': round(total_risk, 2),
                'total_score': round(total_score, 2), 'total_time_min': round(total / 60, 2),
                'path_length': len(pf) + len(pb) - 1,
                'forward_path': forward_path_str, 'backward_path': " → ".join(pb)
            }
            trial_res.append(result)

        if trial_res:
            best = min(trial_res, key=lambda x: x['total_score'])
            results.append(best)
            print(f"[{trial}] {best['station']} (score: {best['total_score']}, 시간(분): {best['total_time_min']}, 위험도: {best['total_risk']})")

    return pd.DataFrame(results), pd.DataFrame(results)

def save_results(df_best, df_all, config):
    os.makedirs(os.path.dirname(config["output_path"]), exist_ok=True)
    if not df_best.empty:
        avg = df_best[['forward_time', 'backward_time', 'total_cost', 'total_risk', 'total_score', 'total_time_min', 'path_length']].mean()
        print("\n=== 전체 평균 ===")
        labels = ['Forward Time', 'Backward Time', '총비용', '위험도', '최종스코어', '총시간(분)', '경로노드수']
        for k, label in zip(avg.index, labels):
            print(f"평균 {label}: {avg[k]:.2f}")

    df_all.to_csv(config["output_path"], index=False, encoding="utf-8-sig")
    print(f"\n✅ 결과 저장 완료: {config['output_path']}")

def main():
    from config.config import init_config_risk_algorithm
    config = init_config_risk_algorithm()
    G, df, node_pos = build_graph(config)
    risk_map = load_risk_map("../data/final_augmented_edges.csv")
    df_best, df_all = simulate(config, G, df, node_pos, risk_map)
    save_results(df_best, df_all, config)

if __name__ == "__main__":
    main()
