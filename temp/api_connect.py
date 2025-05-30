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
filtered_edges = pd.read_csv("../data/final_augmented_edges.csv")

#####api키 입력 
api_keys = [
"91081a5d094607fc61f935fbeb0dabe9",
"4bfd98267b1166816820ff1c8f41bda5",
"286d396d9facfb8a05c42afeed642247",
"3dcf0e9ad154d88a797b84731fa0ddfc",
"2ccfba64c5de3a01ac76c60fcdec8512",
"57daf32046b7eeddfe3d6c7be6fd3910",
"540b83fa0891999b8a834fa29003c7f2",
"26147dce3a5060d8859cedcd544c9529",
"c5a3ecf9e9fdc522986f641960cbc56e",
"f1921ae16bf2028556d763b2ec9b5ecb",

"74553eb0524014c5b3ad2aae1c3164cb",
"6ba1196fdc43882663ac4e8b96c34403",
"12d3d751f0715d497e68a73caac8ee3a",
"f3fcadf4eeac70bf6daa840e849cfdc5",
"bf860c58e71b0891321ea9f758ef8240",
"556243e988bdec394240b03ccdaeda8a",
"356ed9125a3dc1a29edbf8b057b6f320",
"43026fb1f50b3a10c15fba57e132cd74",
"78472c87bcf6fcf7e3d66f2d054d9c98",
"d9353f56c1cdc18811ece23e150eadbe",

'1eaec22f5f25bb655ea8f3b925e4b252',
'c015a192b953b3ff4c529ef04a7e0d87',
'95f6a6fb80518d1aecf866df062cd6a7',
'fa61ad3030cf053b90e6d3f170aeaf85',
'f912ec1fbc0142137966dea82e025e1c',
'a9c3d9507f2672f2ec02a5606243fc7d',
'18e3b6e49900d57354d3a0f4335fd164',
'96f1659b41f12f374367f245d56f8e0f',
'61c728d13f9c62a99ed78ab610642baa',
'2dad5b5bea3bd50a829c285ad75b0a29',

"d49b0c06aa0e51eebc542b4d8e264ec2",
"30f7b3b030316eb40f4dc2d7c0db005e",
"05a3f91d3acfb4a762f25b0dda46f557",
"2cd52728303e61ebbb7c0826bcaba01d",
"017a90587df638dd363e6dcb5f8141a4",
"a1c42dc0a5a38119f3471856ae558447",
"869796f27e2ea6b85d650424a468859f",
"affdf35c096ad46346d59f86628da9fd",
"a6ea790002628d829c654f6d6a3cbadb",
"89c41b0432977e4fa5afcc1237c87c3b",

"50d5ed4b06fe1f8745ede28c374565c0",
"2b59ba4e89fc6d8074ef81f55d4abb63",
"3a00510f3f1aa5fa560c55c1d7859d9d",
"5e1ad3bfb6a156c4c93976578a08109e",
"8697ba3a2d2843012b495b882e866069",
"d5131c99fd7c25f91f6481919dd9a283",
"01c8cdbfb08b86fe26927d4ba8a084e7",
"19213ba2699944e74a5c6b489e2a72a3",
"15086e70435e4b2cb588f8eee1992967",
"24025f86cdef819d8d65c314b9825610"
]
#####

# 🔁 1차 병렬 실행
success_df, fail_df = process_edges_parallel_with_keys(filtered_edges, api_keys, max_workers=10)

# 🔄 실패한 요청 재시도 (다시 병렬 처리)
retry_df, _ = process_edges_parallel_with_keys(fail_df, api_keys, max_workers=10)

# 📦 최종 결과 합치기
assigned_edges = pd.concat([success_df, retry_df], ignore_index=True)
print(f"✅ 최종 성공: {len(assigned_edges)} / {len(filtered_edges)}")

assigned_edges.to_csv("../data/assigned_edges.csv", index=False, encoding='utf-8-sig')


