#####csv입력
import pandas as pd
filtered_edges = pd.read_csv("final_augmented_edges.csv")

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