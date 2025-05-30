import pandas as pd
from future_api_connect import process_edges_parallel_future  # 필요 함수만 import



# 🗝️ API 키 목록
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



# 📁 데이터 불러오기
df = pd.read_csv("../data/50times_gowork_bestonly.csv")

# 출발 시간 설정
departure_time = "202506040800"


# ✅ 미래 경로 duration 병렬 처리
success_df, fail_df = process_edges_parallel_future(df, api_keys, departure_time, max_workers=10)

# 🔁 실패한 요청 재시도
retry_df, _ = process_edges_parallel_future(fail_df, api_keys, departure_time, max_workers=10)

# 📦 결과 합치기
duration_info = pd.concat([success_df, retry_df], ignore_index=True)

# ⏱️ 시간 계산
df['duration'] = duration_info['duration']
df['duration_bus'] = df['duration'] / 60
df['duration_ambulance'] = df['duration'] * 0.692 / 60

# 📉 시간 차이
df['bus_diff'] = df['duration_bus'] - df['total_time_min']
df['ambulance_diff'] = df['duration_ambulance'] - df['total_time_min']

# 📊 절감률 계산
df['bus_saving_percent'] = (df['bus_diff'] / df['duration_bus']) * 100
df['ambulance_saving_percent'] = (df['ambulance_diff'] / df['duration_ambulance']) * 100

# 💾 저장
df.to_csv("../data/50times_day_with_saving_analysis.csv", index=False, encoding="utf-8-sig")

