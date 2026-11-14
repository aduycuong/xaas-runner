# xaas-runner

Worker chạy các run của XaaS Workflows. Upstash Workflow gọi `POST /run/v3` một lần cho mỗi step; runner đọc workflow từ database, gọi node qua service binding và ghi kết quả. Kiến trúc: `xaas-workflows/docs/kien-truc-upstash-cloudflare-hono.md`.

| Route | Ai gọi | Xác thực |
| --- | --- | --- |
| `POST /run/v3` | Upstash Workflow | Chữ ký Upstash (signing keys) |
| `GET /nodes/<tên Worker>/health` | Script deploy node | Bearer `RUNNER_ADMIN_TOKEN` |
| `GET /health` | Uptime monitor | Không |

| File | Nội dung |
| --- | --- |
| `src/index.ts` | Các route, vòng lặp chạy run, đường gọi node |
| `src/engine.ts` | Thân của từng step và toàn bộ SQL |
| `src/logic.ts` | Phần thuần logic: template, điều kiện, phân loại lỗi (có test) |

## Deploy lên Cloudflare

Thứ tự giữa các Worker: **node → runner → ingress**. Runner chỉ deploy được khi mọi Worker trong mục `services` của `wrangler.jsonc` đã tồn tại trên Cloudflare.

### Lần đầu

1. Cài dependency và đăng nhập Cloudflare (một lần cho mỗi máy):

```bash
npm install
```

```bash
npx wrangler login
```

2. Deploy các node đang được bind. Cách thường dùng là chạy script từ repo app, script này cũng tự deploy runner khi có binding mới:

```bash
npm --prefix ../../xaas-workflows run nodes:deploy
```

3. Đặt secret. Mỗi lệnh sẽ hỏi giá trị; nếu Worker chưa tồn tại, wrangler hỏi có tạo không, chọn có.

```bash
npx wrangler secret put DATABASE_URL
```

```bash
npx wrangler secret put QSTASH_TOKEN
```

```bash
npx wrangler secret put QSTASH_CURRENT_SIGNING_KEY
```

```bash
npx wrangler secret put QSTASH_NEXT_SIGNING_KEY
```

```bash
npx wrangler secret put RUNNER_ADMIN_TOKEN
```

| Secret | Lấy ở đâu |
| --- | --- |
| `DATABASE_URL` | Connection string của PlanetScale Postgres, cùng database với app; giữ `?sslmode=verify-full`, bỏ `sslrootcert` |
| `QSTASH_TOKEN`, `QSTASH_CURRENT_SIGNING_KEY`, `QSTASH_NEXT_SIGNING_KEY` | Upstash console, mục QStash |
| `RUNNER_ADMIN_TOKEN` | Tự sinh một chuỗi ngẫu nhiên dài; đặt cùng giá trị vào `.env` của app |
| `QSTASH_URL` (tùy chọn) | Chỉ khi QStash của bạn không ở endpoint mặc định (theo vùng); đặt bằng `wrangler secret put` như trên |

Không đặt `UPSTASH_WORKFLOW_URL` trên Cloudflare: biến đó chỉ dùng ở local khi chạy qua tunnel.

4. Deploy:

```bash
npm run deploy
```

Wrangler in ra URL của Worker, dạng `https://xaas-runner.<subdomain>.workers.dev`. Đây là giá trị của `RUNNER_URL`.

5. Nối với các phần khác:

- Đặt `RUNNER_URL` cho ingress (xem `../ingress/README.md`).
- Trong `.env` của app: `RUNNER_URL` và `RUNNER_ADMIN_TOKEN`, để script deploy node kiểm tra được health.

### Kiểm tra sau khi deploy

```bash
curl https://xaas-runner.<subdomain>.workers.dev/health
```

Kết quả mong đợi: `{"ok":true}`.

```bash
curl -H "Authorization: Bearer <RUNNER_ADMIN_TOKEN>" https://xaas-runner.<subdomain>.workers.dev/nodes/node-core-echo-v1-0-0/health
```

Kết quả mong đợi: `"health":{"ok":true,…}`. Lỗi `node_not_bound` nghĩa là runner đang chạy chưa có binding tới node đó; `admin_token_not_configured` nghĩa là chưa đặt secret `RUNNER_ADMIN_TOKEN`.

Kiểm tra đầy đủ nhất là chạy một workflow thật qua ingress rồi xem bảng `runs`.

### Các lần sau

```bash
npm test
```

```bash
npm run typecheck
```

```bash
npm run deploy
```

- Deploy không gây gián đoạn: request đang xử lý chạy xong trên bản cũ, và Upstash tự gọi lại nếu một step hỏng đúng lúc chuyển bản.
- Secret giữ nguyên qua các lần deploy; chỉ đặt lại khi đổi giá trị.
- Thêm node hoặc version mới thì dùng script `nodes:deploy` ở repo app thay vì sửa `services` bằng tay. Script cũng ghi `../nodes.json`, là danh sách binding mà runner trên Cloudflare đang có.

### Thay đổi làm lệch step

Tên và thứ tự các step (`load`, `node:<id>`, `finish`) là giao kèo với những run đang chạy dở: một run đang chờ retry sẽ chạy tiếp bằng code mới nhưng dùng kết quả step cũ.

- Được phép deploy thẳng: sửa logic bên trong một step, thêm route, thêm binding.
- Đổi tên, bỏ hoặc đổi thứ tự step: thêm route mới (`/run/v4`), giữ `/run/v3` cho tới khi các run cũ chạy hết, và đổi `RUNNER_ROUTE` trong ingress.

### Quay lại bản trước và xem log

```bash
npx wrangler rollback
```

```bash
npx wrangler tail
```

Log cũng xem được trên dashboard Cloudflare (Workers → xaas-runner → Observability). Từng step của một run xem ở Upstash console, mục Workflow, theo run id `wfr_<id trong bảng runs>`.

### Tên miền riêng (tùy chọn)

Muốn dùng tên miền riêng thay cho `workers.dev`, thêm vào `wrangler.jsonc` rồi deploy lại, sau đó cập nhật `RUNNER_URL` ở ingress và app:

```jsonc
"routes": [{ "pattern": "runner.example.com", "custom_domain": true }]
```

## Chạy ở local

Xem `../README.md`. Tóm tắt: chép `.dev.vars.example` thành `.dev.vars`, điền giá trị, đặt `UPSTASH_WORKFLOW_URL` bằng URL của tunnel trỏ về cổng 3000, rồi:

```bash
npm run dev
```

## Realtime (tùy chọn)

Runner ghi trạng thái run và node run lên Firebase Realtime Database qua REST để giao diện cập nhật không cần tải lại. Cần ba secret, cùng giá trị với app: `FIREBASE_CLIENT_EMAIL`, `FIREBASE_PRIVATE_KEY`, `FIREBASE_DATABASE_URL` (`npx wrangler secret put <TÊN>`). Thiếu thì runner bỏ qua việc ghi; ghi lỗi chỉ để lại một dòng log và không làm hỏng step vì Postgres là nguồn chính. Đường dẫn và rule: `xaas-workflows/.ai/rtdb-rules.md`.
