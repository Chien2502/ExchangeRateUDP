# ExchangeRate UDP Unicast (VKU - Đề 1)

Chương trình truyền nhận tỉ giá theo thời gian thực sử dụng giao thức **UDP Unicast** giữa Server và nhiều Client, tối ưu băng thông, xử lý CPU và bộ nhớ.

---

## 🌟 Tính năng chính

- **ExchangeRateServer**:
  - Giao thức UDP Unicast (cổng mặc định: `9876`).
  - Tự động nhận diện và hiển thị địa chỉ IPv4 mạng LAN khi khởi động.
  - Định kỳ mỗi 1 giây sinh tỉ giá ngẫu nhiên của 3 thị trường:
    - **Tokyo (JPY / VND)**
    - **Paris (EUR / VND)**
    - **Seoul (KRW / VND)**
  - Tối ưu mô hình `activeClients`: Tự động xóa client khỏi danh sách khi client bấm `PAUSE`, không lãng phí băng thông gửi tin tới client không có nhu cầu xem.
  - Tắt in log lặp lại mỗi giây để tránh tắc nghẽn I/O console.

- **ExchangeRateClient (Swing GUI)**:
  - Bảng `JTable` 4 cột: `Thời gian (Time)` - `Tokyo` - `Paris` - `Seoul`.
  - **Ghim cố định Header**: Header luôn nằm trên cùng nhờ `JScrollPane`.
  - **Auto-scroll**: Tự động cuộn theo dòng dữ liệu mới nhất.
  - **Chống tràn RAM (FIFO Buffer)**: Giới hạn tối đa 50 dòng, tự động cắt tỉa dòng cũ nhất khi đầy.
  - Các nút điều khiển: **Kết nối (Start)**, **Tạm dừng (Pause)**, **Tiếp tục (Resume)**, và **Xóa bảng (Reset)**.
  - Tự động gửi lệnh hủy đăng ký `UNSUBSCRIBE` khi tắt cửa sổ.

---

## 🎯 Các điểm tối ưu

1. **Băng thông (Bandwidth)**:
   - Dùng định dạng chuỗi CSV phân tách (`;`) thay cho Java Object Serialization (`ObjectOutputStream`), giảm kích thước gói tin từ ~250-300 bytes xuống chỉ còn **~30-35 bytes** (tiết kiệm >85% băng thông).
   - Khi client bấm Pause, Server ngừng gửi hoàn toàn.

2. **Xử lý (CPU)**:
   - Server sinh tỉ giá và serialize ra byte array đúng **1 lần duy nhất** cho mỗi chu kỳ 1s, rồi tái sử dụng buffer gửi cho tất cả client.
   - Vòng lặp gửi tin chạy $O(N_{active})$ trực tiếp, không cần kiểm tra cờ trạng thái hay lọc Stream.
   - Client tách biệt luồng nhận UDP ngầm và luồng cập nhật giao diện (`SwingUtilities.invokeLater`).

3. **Lưu trữ & Bộ nhớ (Memory/RAM)**:
   - Tái sử dụng mảng byte buffer và `DatagramPacket`, tránh tạo mới liên tục gây áp lực Garbage Collection.
   - Giới hạn 50 dòng trên giao diện Client, tránh tràn bộ nhớ Heap khi chạy lâu dài.

---

## 🚀 Hướng dẫn chạy

### 1. Biên dịch
```powershell
javac -d bin src/module-info.java src/exchangerate/*.java
```

### 2. Khởi động Server
```powershell
java -cp bin exchangerate.ExchangeRateServer
```

### 3. Khởi động Client
```powershell
java -cp bin exchangerate.ExchangeRateClient
```
*(Nếu chạy trên các máy khác nhau trong cùng mạng LAN, nhập IP LAN của máy Server vào ô **Server Host** ở Client)*.
