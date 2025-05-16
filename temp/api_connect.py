import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
import time
import random
from collections import defaultdict



# 전역 블랙리스트와 블락 타이머
blacklisted_keys = set()
blacklist_timers = defaultdict(float)  # key_idx: timestamp when it can be retried

# 기본 파라미터
API_RETRY_COOLDOWN = 120  # 429 걸리면 2분간 블랙
MIN_SLEEP = 0.05
MAX_SLEEP = 0.3

def get_route_info_with_rotation(origin, destination, api_keys, key_index, u, v):
    attempts = 0
    total_keys = len(api_keys)

    while attempts < total_keys:
        key_idx = (key_index + attempts) % total_keys

        # 블랙리스트 타이머 확인
        if key_idx in blacklisted_keys:
            now = time.time()
            if now < blacklist_timers[key_idx]:
                attempts += 1
                continue
            else:
                blacklisted_keys.remove(key_idx)

        api_key = api_keys[key_idx]
        url = "https://apis-navi.kakaomobility.com/v1/directions"
        headers = {"Authorization": f"KakaoAK {api_key}"}
        params = {
            "origin": f"{origin[0]},{origin[1]},angle=270",
            "destination": f"{destination[0]},{destination[1]}",
            "summary": "true",
            "priority": "RECOMMEND",
            "car_fuel": "GASOLINE",
            "car_hipass": "false",
            "alternatives": "false",
            "road_details": "false"
        }

        try:
            response = requests.get(url, headers=headers, params=params, timeout=10)
            if response.status_code == 200:
                data = response.json()
                if 'routes' in data and data['routes'] and 'sections' in data['routes'][0]:
                    section = data['routes'][0]['sections'][0]
                    duration = section['duration']
                    print(f"[✅ Success] {origin} → {destination} | {duration:.1f} sec | API-{key_idx+1}")
                    return {
                        'u': u, 'v': v,
                        'u_x': origin[0], 'u_y': origin[1],
                        'v_x': destination[0], 'v_y': destination[1],
                        'duration': duration
                    }
                else:
                    print(f"[⚠️ No sections] {origin} → {destination} | API-{key_idx+1}")
            else:
                print(f"[❌ HTTP {response.status_code}] {origin} → {destination} | API-{key_idx+1}")
                if response.status_code == 429:
                    print(f"[🚫 Rate Limited] API-{key_idx+1} temporarily blacklisted.")
                    blacklisted_keys.add(key_idx)
                    blacklist_timers[key_idx] = time.time() + API_RETRY_COOLDOWN
        except Exception as e:
            print(f"[❗ Exception] {origin} → {destination} | API-{key_idx+1} | {e}")

        attempts += 1
        time.sleep(random.uniform(MIN_SLEEP, MAX_SLEEP))  # ✅ Rate 분산을 위해 sleep 랜덤 설정

    print(f"[❌ All APIs failed] {origin} → {destination}")
    return None

def process_edges_parallel_with_keys(edges_df, api_keys, max_workers=5):
    results = []
    failures = []

    def task(row_index_row):
        idx, row = row_index_row
        origin = (row['u_x'], row['u_y'])
        destination = (row['v_x'], row['v_y'])
        result = get_route_info_with_rotation(origin, destination, api_keys, idx, row['u'], row['v'])
        return result if result else row  # 실패한 경우 row 자체 반환

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        indexed_rows = list(edges_df.iterrows())
        futures = [executor.submit(task, pair) for pair in indexed_rows]

        for future in as_completed(futures):
            try:
                result = future.result()
                if isinstance(result, dict):
                    results.append(result)
                else:
                    failures.append(result)
            except Exception as e:
                print(f"[❗ Future Exception] {e}")

    return pd.DataFrame(results), pd.DataFrame(failures)


#####csv입력
import pandas as pd
filtered_edges = pd.read_csv("filtered_edges.csv")

#####api키 입력 
api_keys = [
]
#####

# 🔁 1차 병렬 실행
success_df, fail_df = process_edges_parallel_with_keys(filtered_edges, api_keys, max_workers=10)

# 🔄 실패한 요청 재시도 (다시 병렬 처리)
retry_df, _ = process_edges_parallel_with_keys(fail_df, api_keys, max_workers=10)

# 📦 최종 결과 합치기
assigned_edges = pd.concat([success_df, retry_df], ignore_index=True)
print(f"✅ 최종 성공: {len(assigned_edges)} / {len(filtered_edges)}")

