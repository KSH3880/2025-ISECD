import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
import time
import random
from collections import defaultdict
import argparse
from config.config import config_api, get_api_keys

## 실행 방법
## 현재 시간 기준
#python api_connect.py --mode real
## 미래 시간 기준
#python api_connect.py --mode future --departure_time 202507010800

# 블랙리스트 관리
blacklisted_keys = set()
blacklist_timers = defaultdict(float)

API_RETRY_COOLDOWN = 120
MIN_SLEEP = 0.05
MAX_SLEEP = 0.3

# ✅ 현재 경로 요청
def get_route_info_with_rotation(origin, destination, api_keys, key_index, u, v):
    attempts = 0
    total_keys = len(api_keys)

    while attempts < total_keys:
        key_idx = (key_index + attempts) % total_keys

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
            "priority": "TIME",
            "car_fuel": "GASOLINE",
            "car_hipass": "false",
            "alternatives": "false",
            "road_details": "false",
            "road_event": "2"
        }

        try:
            response = requests.get(url, headers=headers, params=params, timeout=10)
            if response.status_code == 200:
                data = response.json()
                if 'routes' in data and data['routes'] and 'sections' in data['routes'][0]:
                    section = data['routes'][0]['sections'][0]
                    duration = section['duration']
                    print(f"[✅ 현재 성공] {origin} → {destination} | {duration:.1f} sec | API-{key_idx+1}")
                    return {
                        'u': u, 'v': v,
                        'u_x': origin[0], 'u_y': origin[1],
                        'v_x': destination[0], 'v_y': destination[1],
                        'duration': duration
                    }
            else:
                print(f"[❌ HTTP {response.status_code}] {origin} → {destination} | API-{key_idx+1}")
                if response.status_code == 429:
                    print(f"[🚫 Rate Limited] API-{key_idx+1}")
                    blacklisted_keys.add(key_idx)
                    blacklist_timers[key_idx] = time.time() + API_RETRY_COOLDOWN
        except Exception as e:
            print(f"[❗ Exception] {origin} → {destination} | API-{key_idx+1} | {e}")

        attempts += 1
        time.sleep(random.uniform(MIN_SLEEP, MAX_SLEEP))

    print(f"[❌ All APIs failed] {origin} → {destination}")
    return None

# ✅ 미래 경로 요청
def get_future_route_info_with_rotation(origin, destination, api_keys, key_index, u, v, departure_time):
    attempts = 0
    total_keys = len(api_keys)

    while attempts < total_keys:
        key_idx = (key_index + attempts) % total_keys

        if key_idx in blacklisted_keys:
            now = time.time()
            if now < blacklist_timers[key_idx]:
                attempts += 1
                continue
            else:
                blacklisted_keys.remove(key_idx)

        api_key = api_keys[key_idx]
        url = "https://apis-navi.kakaomobility.com/v1/future/directions"
        headers = {
            "Authorization": f"KakaoAK {api_key}",
            "Content-Type": "application/json"
        }
        params = {
            "origin": f"{origin[0]},{origin[1]},angle=270",
            "destination": f"{destination[0]},{destination[1]}",
            "departure_time": departure_time,
            "summary": "true",
            "priority": "TIME",
            "car_fuel": "GASOLINE",
            "car_hipass": "false",
            "alternatives": "false",
            "road_details": "false",
            "roadevent": "2"
        }

        try:
            response = requests.get(url, headers=headers, params=params, timeout=10)
            if response.status_code == 200:
                data = response.json()
                if 'routes' in data and data['routes'] and 'sections' in data['routes'][0]:
                    section = data['routes'][0]['sections'][0]
                    duration = section['duration']
                    print(f"[✅ 미래 성공] {origin} → {destination} | {duration:.1f} sec | API-{key_idx+1}")
                    return {
                        'u': u, 'v': v,
                        'u_x': origin[0], 'u_y': origin[1],
                        'v_x': destination[0], 'v_y': destination[1],
                        'duration': duration,
                    }
        except Exception as e:
            print(f"[❗ Exception] {origin} → {destination} | API-{key_idx+1} | {e}")

        attempts += 1
        time.sleep(random.uniform(MIN_SLEEP, MAX_SLEEP))

    print(f"[❌ All APIs failed] {origin} → {destination}")
    return None

def process_edges_parallel(edges_df, api_keys, mode, departure_time=None, max_workers=10):
    results, failures = [], []

    def task(row_index_row):
        idx, row = row_index_row
        origin = (row['u_x'], row['u_y'])
        destination = (row['v_x'], row['v_y'])
        if mode == 'real':
            return get_route_info_with_rotation(origin, destination, api_keys, idx, row['u'], row['v']) or row
        else:
            return get_future_route_info_with_rotation(origin, destination, api_keys, idx, row['u'], row['v'], departure_time) or row

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(task, item) for item in edges_df.iterrows()]
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


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--mode', type=str, choices=['real', 'future'], help='기본값: real')
    parser.add_argument('--departure_time', type=str, help='미래 출발 시간 지정 (예: 202507010800)')
    args = parser.parse_args()

    cfg = config_api()  
    api_keys = get_api_keys() 
    
    mode = args.mode if args.mode else cfg.get("DEFAULT_MODE", "real")
    departure_time = args.departure_time if args.departure_time else cfg["DEFAULT_DEPARTURE_TIME"]

    df = pd.read_csv(cfg["INPUT_CSV_PATH"])

    success_df, fail_df = process_edges_parallel(df, api_keys, mode, departure_time, max_workers=cfg["MAX_WORKERS"])
    retry_df, _ = process_edges_parallel(fail_df, api_keys, mode, departure_time, max_workers=cfg["MAX_WORKERS"])

    final_df = pd.concat([success_df, retry_df], ignore_index=True)
    print(f"✅ 최종 성공: {len(final_df)} / {len(df)}")

    output_path = cfg["OUTPUT_CSV_PATH_FUTURE"] if mode == "future" else cfg["OUTPUT_CSV_PATH_REAL"]
    final_df.to_csv(output_path, index=False, encoding="utf-8-sig")
    print(f"📁 저장 완료 → {output_path}")

