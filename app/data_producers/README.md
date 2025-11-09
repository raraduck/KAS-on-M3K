## SMD (Server Machine Dataset) Features (38)

|id| 컬럼명                | 추정 의미                                                               |
|------| ------------------ | ------------------------------------------------------------------- |
|`col_0`| `cpu_r`            | CPU 사용율이나 CPU 리소스 대비 사용률(“cpu rate” 또는 CPU가 사용 중인 비율)               |
|`col_1`| `load_1`           | 최근 1분간 시스템의 평균부하(load average) — CPU 대기 프로세스 포함. ([hosting.com][1]) |
|`col_2`| `load_5`           | 최근 5분간의 평균 시스템 부하                                                   |
|`col_3`| `load_15`          | 최근 15분간의 평균 시스템 부하                                                  |
|`col_4`| `mem_shmem`        | 공유 메모리(shared memory) 사용량                                           |
|`col_5`| `mem_u`            | 사용 중 메모리(“memory used”) 양                                           |
|`col_6`| `mem_u_e`          | 메모리 사용량 중 “evictable” 또는 “이벤트 기반으로 회수 가능한” 메모리량일 가능성 있음             |
|`col_7`| `total_mem`        | 전체 메모리 용량 또는 전체 메모리 중 모니터링 대상 총량                                    |
|`col_8`| `disk_q`           | 디스크 큐 길이(disk queue) — 입출력 요청이 대기 중인 수                              |
|`col_9`| `disk_r`           | 디스크에서 읽은(read) 데이터량 혹은 읽기 요청 수                                      |
|`col_10`| `disk_rb`          | 디스크에서 읽은 바이트(read bytes)                                            |
|`col_11`| `disk_svc`         | 서비스 시간(service time) 혹은 I/O 요청을 처리하는 데 걸린 평균 시간                     |
|`col_12`| `disk_u`           | 디스크 사용률(disk utilization) — I/O 바쁜 비율                               |
|`col_13`| `disk_w`           | 디스크 쓰기(write) 요청 수 또는 데이터량                                          |
|`col_14`| `disk_wa`          | 디스크 I/O가 대기 중인 시간이 쓰기(wait) 비율 또는 “write-active wait” 비율            |
|`col_15`| `disk_wb`          | “write busy” 항목 또는 쓰기 바이트(write bytes)로 유추됨                         |
|`col_16`| `si`               | swap-in(스왑인) 또는 시스템에서 메모리 부족 시 디스크로부터 메모리 페이지를 메인 메모리로 불러오는 양       |
|`col_17`| `so`               | swap-out(스왑아웃) — 메모리에서 디스크로 페이지가 넘어간 양                              |
|`col_18`| `eth1_fi`          | 네트워크 인터페이스 eth1에서 들어오는 패킷 수 혹은 바이트 수 (fi = from in)                 |
|`col_19`| `eth1_fo`          | 인터페이스 eth1에서 나가는 패킷 수 혹은 바이트 수 (fo = from out)                      |
|`col_20`| `eth1_pi`          | 인터페이스 eth1에서 받은 패킷 수(packet in)                                     |
|`col_21`| `eth1_po`          | 인터페이스 eth1에서 보낸 패킷 수(packet out)                                    |
|`col_22`| `tcp_tw`           | TCP에서 TIME_WAIT 상태의 연결 수                                            |
|`col_23`| `tcp_use`          | 사용 중인 TCP 연결 수 또는 TCP 연결 사용률                                        |
|`col_24`| `active_opens`     | TCP의 `active open` 시도 수 — 클라이언트가 세션을 열기 위한 시도                       |
|`col_25`| `curr_estab`       | 현재 확립(established)된 TCP 연결 수                                        |
|`col_26`| `in_errs`          | 들어오는(incoming) TCP/네트워크 패킷 중 오류(err) 수                              |
|`col_27`| `in_segs`          | 들어온 세그먼트(segment) 수 — TCP/IP 계층에서 받은 세그먼트 수                         |
|`col_28`| `listen_overflows` | 수신 대기열(listen queue)이 “overflow”된 횟수 — 서버가 접속을 처리하지 못해 유실된 연결 시도 수  |
|`col_29`| `out_rsts`         | 나가는(outgoing) RST(reset) 패킷 수 — TCP 연결이 강제로 종료된 횟수                  |
|`col_30`| `out_segs`         | 나간 세그먼트 수 — TCP/IP 계층에서 보낸 세그먼트 수                                   |
|`col_31`| `passive_opens`    | 수동(passive) 오픈 — 서버가 연결을 수용(listen 상태에서 accept)한 수                  |
|`col_32`| `retransegs`       | 재전송된 세그먼트 수(retransmitted segments) — 네트워크나 연결 품질 문제로 인한 재전송        |
|`col_33`| `tcp_timeouts`     | TCP 타임아웃이 발생한 횟수 — 연결이 시간 내에 응답을 못했거나 유지되지 못한 횟수                    |
|`col_34`| `udp_in_dg`        | UDP에서 들어온 데이터그램(datagram) 수                                         |
|`col_35`| `udp_out_dg`       | UDP에서 나간 데이터그램 수                                                    |
|`col_36`| `udp_rcv_buf_errs` | UDP 수신 버퍼(receive buffer) 오류수 — 수신 버퍼가 꽉 차거나 처리되지 못한 오류             |
|`col_37`| `udp_snd_buf_errs` | UDP 송신 버퍼(send buffer) 오류수 — 전송 측 버퍼 문제로 인해 발생한 오류                  |

[1]: https://kb.hosting.com/docs/understanding-system-load-averages?utm_source=chatgpt.com "Understanding system load averages"



## Group Categorization

| 그룹                    | 관련 feature                                                                                       | 설명                            | anomaly 영향                                  |
| --------------------- | ------------------------------------------------------------------------------------------------ | ----------------------------- | ------------------------------------------- |
| 🧠 **CPU/Load**       | `cpu_r`, `load_1`, `load_5`, `load_15`                                                           | 시스템의 순간적 부하, 병목 신호 반영         | ✅ 매우 높음 — 과부하, 프로세스 폭주                      |
| 💾 **Memory**         | `mem_shmem`, `mem_u`, `mem_u_e`, `total_mem`, `si`, `so`                                         | 메모리 누수, 스왑 발생 감지              | ✅ 높음 — 메모리 leak, swap 발생                    |
| 💽 **Disk I/O**       | `disk_q`, `disk_r`, `disk_rb`, `disk_svc`, `disk_u`, `disk_w`, `disk_wa`, `disk_wb`              | I/O 병목, 디스크 포화도               | ✅ 높음 — disk queue 급등, service time 비정상      |
| 🌐 **Network (eth1)** | `eth1_fi`, `eth1_fo`, `eth1_pi`, `eth1_po`                                                       | 트래픽 패턴, burst, network outage | ⚙️ 중간 — burst나 포화 구간                        |
| 🔗 **TCP 상태**         | `tcp_tw`, `tcp_use`, `active_opens`, `curr_estab`, `passive_opens`, `retransegs`, `tcp_timeouts` | 연결 시도, 재전송, timeout           | ✅ 높음 — connection storm, failure, DDoS-like |
| 📡 **UDP 통계**         | `udp_in_dg`, `udp_out_dg`, `udp_rcv_buf_errs`, `udp_snd_buf_errs`                                | UDP overflow, buffer 오류       | ⚙️ 중간 — 특정 anomaly만 반응                      |


## Informativeness

| 순위  | 주요 feature                                   | 이유              | 추천 분석 방식         |
| --- | -------------------------------------------- | --------------- | ---------------- |
| 1️⃣ | `cpu_r`, `load_1`, `load_5`, `load_15`       | 부하 급등, 과도한 프로세스 | PCA / AE         |
| 2️⃣ | `mem_u`, `mem_shmem`, `si`, `so`             | 메모리 누수, swap 폭주 | PCA / AE         |
| 3️⃣ | `disk_q`, `disk_svc`, `disk_u`               | I/O 대기, 디스크 포화  | PCA / AE         |
| 4️⃣ | `tcp_timeouts`, `retransegs`, `active_opens` | 통신 오류, 세션 타임아웃  | AE / MI          |
| 5️⃣ | `udp_rcv_buf_errs`, `udp_snd_buf_errs`       | 버퍼 오류           | AE (fine-tuning) |

### 1. PCA
```python
from sklearn.decomposition import PCA
import pandas as pd

X = df[new_column_names].fillna(0)
pca = PCA(n_components=10)
pca.fit(X)

importance = abs(pca.components_[0])  # 첫 번째 주성분의 기여도
feature_importance = pd.Series(importance, index=new_column_names).sort_values(ascending=False)
print(feature_importance.head(10))
```

### 2. Mutual Information
```python
from sklearn.feature_selection import mutual_info_classif
mi = mutual_info_classif(X, y)
pd.Series(mi, index=new_column_names).sort_values(ascending=False)
```
