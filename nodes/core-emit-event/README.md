# Emit event

Phát một event có tên và payload. Mọi trigger **custom event** của workspace đang nghe đúng tên đó, cùng môi trường (env) với run này, sẽ chạy workflow của nó. Dùng để nối các workflow với nhau: workflow này làm xong một việc thì báo, workflow khác nhận và làm tiếp, với đúng dữ liệu bạn chọn gửi đi.

Node không chờ các workflow kia chạy xong: nó trả về ngay danh sách run đã được tạo (hoặc lý do không tạo).

## Input

| Ô | Mặc định | Điền gì |
| --- | --- | --- |
| `event` | (bắt buộc) | Tên event, ví dụ `task.created`. Chữ, số và `. _ : -`. Trigger nghe đúng tên này (phân biệt hoa thường) |
| `payload` | `{}` | Dữ liệu gửi đi, một JSON object (tối đa 256 KB). Ghép từ node trước bằng chế độ **{ } Expression**, ví dụ `{{ { "task": nodes.ai_1.output.text, "chat_id": trigger.body.chat.id } }}` |

## Bên nhận

Ở trang **Triggers**, tạo trigger loại **Event**, chọn **A run emits an event** và gõ đúng tên event. Run được tạo đọc:

- `{{ trigger.body.payload }}`: payload node này gửi, ví dụ `{{ trigger.body.payload.task }}`.
- `{{ trigger.body.source.workflow.slug }}`, `{{ trigger.body.source.run.no }}`, `{{ trigger.body.source.node.id }}`: ai đã phát.
- `{{ trigger.body.event }}`, `{{ trigger.body.id }}`, `{{ trigger.body.env }}`.

Filter và input mapping của trigger dùng như mọi trigger khác, ví dụ filter `{{ trigger.body.payload.is_task }}` equals `true`, mapping `task` = `{{ trigger.body.payload.task }}`.

## Output

```json
{
  "event": "task.created",
  "id": "0196f1c2-…",
  "env": "production",
  "started": 1,
  "triggers": [
    {
      "trigger": { "id": "…", "name": "Ghi nhận nhiệm vụ" },
      "workflow": { "id": "…", "slug": "ghi-nhan-nhiem-vu", "name": "Ghi nhận nhiệm vụ" },
      "outcome": "fired",
      "reason": null,
      "run": { "id": "…", "no": 137, "queued": false }
    }
  ]
}
```

- `outcome`: `fired` (đã tạo run, `queued` cho biết run đang chờ slot), `rejected` (filter không khớp, input sai schema, hàng chờ đầy…) hoặc `skipped` (workflow chưa publish, đang đầy và trigger chọn Skip, hoặc `loop_detected`).
- Không có trigger nào nghe tên này thì `triggers` rỗng và `started` là `0`: node vẫn thành công. Gõ sai tên sẽ không báo lỗi, hãy xem `started` trong output của run.

## Lưu ý

- **Cùng môi trường:** run ở `staging` chỉ chạy các trigger đặt `staging`; run ở `production` chỉ chạy trigger `production`.
- **Chạy thử trong editor cũng phát thật:** test node này hay test cả workflow đều tạo run thật ở các workflow nghe event (theo env của lần test, mặc định `staging`).
- **Không vòng lặp:** workflow đã có trong chuỗi run dẫn tới đây (kể cả chính workflow đang phát) không được chạy lại, và chuỗi dài tối đa 5 run. Trigger đó ghi `loop_detected`.
- **Retry:** node thử lại khi không gửi được; run đã tạo ở lần trước không bị tạo lại.
