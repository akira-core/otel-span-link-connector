我現在有一個監控／觀測性架構，請你先完整理解我的需求與約束，再幫我產出一個詳細且可落地的執行計畫，重點在設計與高階方案，而不是實作細節。

一、現有架構與元件
我已經有一套 OpenTelemetry Collector 監控架構：
應用程式送 traces 到 Otel Collector。
Collector 使用 servicegraph connector，從 traces 裡推導服務間呼叫關係，產出 service graph metrics。
這些 metrics 會被送到 VictoriaMetrics。
目前的 servicegraph 行為：
主要依據 parent/child 關係 + span kind（CLIENT/SERVER） 來建立「請求邊」（誰呼叫誰）。
產生類似 traces_service_graph_request_total 之類的 metrics，用來畫 service graph。
二、新需求總結（核心目標）
我的應用程式開始使用 OpenTelemetry span links 來表達跨服務、跨 trace 的關聯，特別是 透過 queue（例如 Kafka）實作的非同步流程：
e.g. Producer 服務對 queue 的 enqueue span 和 Consumer 服務對同一筆訊息的 dequeue / 處理 span 之間，加上一條 span link。
我希望 span links 也能成為 service graph 的一部分，特別是：
顯示 多對多、fan-out / fan-in 這種 pattern（像是「服務 A fan-out 多筆訊息給服務 B/C…」，或「多個服務都把東西丟到同一個 aggregator」）。
重要約束：我不想新增一套完全不同名字的 metrics，而是希望：
仍然使用現有 servicegraph connector 輸出的 traces_service_graph_request_total 這類 metrics 名稱。
但在這組 metrics 裡，同時包含：
由 parent/child 推導出的「request 邊」
由 span links 推導出的「link 邊」
也就是：希望你設計一個新的元件（例如 span-links-to-servicegraph），產出與 servicegraph 相容的 metrics，可以共用同一 metric 名稱，只是用 label 來區分來源。
三、我對 metrics 的具體偏好
Metric 名稱：沿用 traces_service_graph_request_*（例如 _total, _latency_bucket 等），不要再新增一個完全不同的 metric 名稱族。
我可以接受在 label 上新增一個欄位來區分來源，舉例：
edge_relation = "request" | "link"
其中：
"request"：代表傳統 servicegraph connector 產生的邊（parent/child + CLIENT/SERVER）
"link"：代表由 span links 產生的邊（queue / fan-out / fan-in 等）
其他 labels 可以延伸自原本 servicegraph 的 labels，例如：
src_service, dst_service
額外可選 labels：messaging_system, messaging_destination, link_type（如 "queue_enq_deq", "retry", "batch_input" 等）
四、Collector 端新元件：span-links-to-servicegraph 的設計原則
我希望你幫我設計一個在 Otel Collector 裡運作的元件（可以是 processor 或 connector，名稱暫稱 span-links-to-servicegraph），它的設計原則如下：

最低需求：只靠 span link 的基本欄位就能運作

假設 Collector 能看到這些欄位：
每個 span：
span.trace_id
span.span_id
span.attributes["service.name"]（實務上 servicegraph 本來就依賴這個）
span.links[]：
每個 link 至少有 link.trace_id, link.span_id（還有 flags/state）
即使使用者沒有在 link 或 span 上塞任何額外 attributes，這個元件也要能：
先在一個批次(batch)內建立：
map[(trace_id, span_id)] -> span_info{service.name, attributes...}
然後對每個 span S（當作目的端 dst_span），遍歷其 links：
用 (link.trace_id, link.span_id) 從 map 找到來源端 src_span
若找到：
src_service = src_span.service.name
dst_service = dst_span.service.name
產生一條「link edge」，並對 traces_service_graph_request_total{edge_relation="link", ...} 做累加
換句話說：只要有 trace_id / span_id，就能靠 Collector 內部的暫存與 lookup 運作起來。
加值使用：利用更多欄位豐富 metrics

如果使用者在 span 或 link 上有更多 attributes，元件應該「善加利用」但不強制依賴。例如：
link.attributes["src_service"]：若存在，優先視為來源服務名，否則用 map lookup 得到的 src_span.service.name。
messaging.system, messaging.destination, messaging.message_id 等，可來自：
link.attributes
或 span.attributes（如果 link 上沒有）
link_type：使用者可以自訂，如 "queue_enq_deq", "retry", "batch_input"…
這些額外資訊可以作為 metrics 的 labels，用來在 Grafana 上切視圖。
與原有 servicegraph 共存

不移除也不修改現有 servicegraph connector 的行為，讓它繼續產出：
traces_service_graph_request_total{edge_relation="request", ...}
新元件 span-links-to-servicegraph 只負責：
從 traces 中讀取 spans + links
產出同名 metric：
traces_service_graph_request_total{edge_relation="link", ...}
最終在 VictoriaMetrics 中：
同一個 metric 名稱 traces_service_graph_request_total 有兩種 edge_relation：
"request"：parent/child 的呼叫圖
"link"：span link 的 queue / fan-out / fan-in 關係圖
五、應用程式與 span link 的行為（請你幫我用架構圖／文字再整理）
我目前對 span link 的理解如下，請你在計畫裡幫我用架構圖或流程圖再整理一次，確認設計是否合理：

在 queue 情境中（以 Kafka 為例）：
Producer 端：
建立 enqueue span A，service.name = "producer-service"，並寫入一些 messaging.* attributes。
取出 SpanContext_A（含 trace_id_A, span_id_A），透過 propagator 寫入 message header（例如 traceparent）。
Consumer 端：
收到訊息時，從 header 讀出 SpanContext_A。
建立處理訊息的 span B（通常作為新 trace 的 root），並在 B 上新增一個 link 指向 A：
links = [ Link(context = SpanContext_A, attributes = { messaging.*, link_type="queue_enq_deq", ... }) ]
Collector 收到這些 spans 後：
透過 span-links-to-servicegraph，從 B 及其 link 找到 A 的 service，建立：
src_service = producer-service
dst_service = consumer-service
並把這條關係作為 traces_service_graph_request_total{edge_relation="link", ...} 的一條邊。
六、我要你產出的東西
請你根據以上所有需求與約束，產出一份：

高階設計與執行計畫：

分段說明：
應用程式（Producer/Consumer）如何標準化產生 spans + links（包含推薦的 attributes 命名規範）。
Otel Collector 新元件 span-links-to-servicegraph 的職責與 data flow。
如何與現有 servicegraph connector 與 VictoriaMetrics 整合。
盡量使用條列與簡潔、明確的步驟來描述。
metrics 與 labels 設計：

詳細列出 traces_service_graph_request_total 在「request」與「link」兩種 edge_relation 下，建議有哪些 labels，以及這些 labels 的來源（span.attributes 或 link.attributes）。
如有 latency / fan-out / fan-in 等額外 metrics，也請提出建議（但仍盡量與現有命名對齊）。
（選用）簡單的偽代碼／流程圖：

對 span-links-to-servicegraph 的核心演算法給一段偽代碼或流程描述：
如何建立 (trace_id, span_id) -> span 的 index
如何從 spans + links 映射出 src_service / dst_service
如何產出 metrics（含 label 組合與累加邏輯）
請用清晰的技術中文回答，並保持結構化（使用標題與條列），讓我可以直接拿這份結果當作設計文件或後續實作的藍本。
