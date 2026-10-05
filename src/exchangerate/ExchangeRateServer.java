package exchangerate;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.util.Enumeration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * ExchangeRateServer: Server UDP Unicast định kỳ gửi tỉ giá tới các Client mỗi giây.
 * 
 * =========================================================================
 * CÁC ĐIỂM TỐI ƯU CỦA SERVER:
 * 1. TỐI ƯU DANH SÁCH ACTIVE CLIENTS (LOẠI BỎ BIẾN TRẠNG THÁI):
 *    - Khi Client PAUSE (hoặc UNSUBSCRIBE): Server lập tức REMOVE client đó
 *      khỏi activeClients Map.
 *    - Khi Client RESUME (hoặc SUBSCRIBE): Server THÊM LẠI client vào activeClients.
 *    - LỢI ÍCH:
 *      + Loại bỏ hoàn toàn các biến cờ kiểm tra trạng thái (như boolean paused).
 *      + Vòng lặp gửi tin chỉ cần kiểm tra activeClients.isEmpty() trong O(1).
 *      + Khi duyệt gửi, duyệt thẳng qua danh sách activeClients mà KHÔNG CẦN
 *        bất kỳ câu lệnh if hay thao tác lọc (filter/stream) nào!
 * 
 * 2. TỐI ƯU BĂNG THÔNG (BANDWIDTH):
 *    - Gửi Unicast có chọn lọc: Server chỉ gửi tới các client đang ACTIVE.
 *    - Khi client Pause, client bị xóa khỏi danh sách -> 0 bytes lãng phí trên mạng.
 *    - Payload chuỗi CSV siêu nhẹ (~30-35 bytes), tiết kiệm hơn 85% so với Object Serialization.
 * 
 * 3. TỐI ƯU XỬ LÝ (PROCESSING / CPU):
 *    - One-Time Serialization: Sinh tỉ giá và đóng gói ra byte array 1 LẦN DUY NHẤT
 *      mỗi giây, tái sử dụng buffer cho tất cả client.
 *    - ScheduledExecutorService chạy nền với độ chính xác cao.
 *    - ConcurrentHashMap hỗ trợ truy cập đa luồng an toàn, lock-free.
 * 
 * 4. TỐI ƯU LƯU TRỮ & BỘ NHỚ (STORAGE / MEMORY):
 *    - Tái sử dụng DatagramPacket và mảng byte trong các vòng lặp gửi/nhận.
 *    - Không lưu trữ session rác của các client đã Pause/ngắt kết nối.
 * =========================================================================
 */
public class ExchangeRateServer {

    private final int port;
    private DatagramSocket socket;

    // Danh sách các Client ĐANG HOẠT ĐỘNG (nhận tin)
    // Key: "ip:port", Value: ClientSession
    private final ConcurrentHashMap<String, ClientSession> activeClients = new ConcurrentHashMap<>();

    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private volatile boolean isRunning = true;

    /**
     * Đại diện phiên gửi tin của một Client đang active (chỉ lưu thông tin địa chỉ mạng).
     * Loại bỏ hoàn toàn biến trạng thái 'paused' để tối ưu bộ nhớ và xử lý.
     */
    public static class ClientSession {
        private final InetAddress address;
        private final int port;

        public ClientSession(InetAddress address, int port) {
            this.address = address;
            this.port = port;
        }

        public InetAddress getAddress() {
            return address;
        }

        public int getPort() {
            return port;
        }

        public String getClientId() {
            return address.getHostAddress() + ":" + port;
        }
    }

    public ExchangeRateServer(int port) {
        this.port = port;
    }

    public void start() {
        try {
            socket = new DatagramSocket(port);
            printServerNetworkInfo();

            // 1. Khởi động task định kỳ 1 giây để broadcast tỉ giá tới active clients
            startBroadcaster();

            // 2. Luồng chính lắng nghe các lệnh từ Client (SUBSCRIBE, PAUSE, RESUME, UNSUBSCRIBE)
            listenForClientCommands();

        } catch (SocketException e) {
            System.err.println("Lỗi khởi tạo DatagramSocket: " + e.getMessage());
        } finally {
            stop();
        }
    }

    /**
     * In ra các địa chỉ IP mạng LAN và Port của Server để Client dễ dàng kết nối
     */
    private void printServerNetworkInfo() {
        System.out.println("=========================================================");
        System.out.println("       EXCHANGE RATE SERVER (UDP UNICAST) ĐÃ KHỞI CHẠY    ");
        System.out.println("  - Cổng lắng nghe (Port): " + port);
        System.out.println("  - Danh sách IP để Client kết nối (Mạng LAN):");

        boolean foundLanIp = false;
        try {
            Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
            while (interfaces.hasMoreElements()) {
                NetworkInterface iface = interfaces.nextElement();
                // Bỏ qua loopback (127.0.0.1), card tắt hoặc card ảo
                if (iface.isLoopback() || !iface.isUp() || iface.isVirtual()) {
                    continue;
                }
                Enumeration<InetAddress> addresses = iface.getInetAddresses();
                while (addresses.hasMoreElements()) {
                    InetAddress addr = addresses.nextElement();
                    if (addr instanceof Inet4Address && !addr.isLoopbackAddress()) {
                        System.out.printf("      👉 IP LAN: %-15s (%s)%n", addr.getHostAddress(), iface.getDisplayName());
                        foundLanIp = true;
                    }
                }
            }
            if (!foundLanIp) {
                System.out.println("      👉 IP: " + InetAddress.getLocalHost().getHostAddress());
            }
        } catch (Exception e) {
            System.out.println("      👉 IP: 127.0.0.1 (localhost)");
        }

        System.out.println("      👉 Kết nối nội bộ (cùng máy): localhost hoặc 127.0.0.1");
        System.out.println("  - Hướng dẫn kết nối Client trong mạng LAN:");
        System.out.println("      Tại máy Client -> Nhập IP LAN ở trên vào ô 'Server Host' và port " + port);
        System.out.println("  - Chế độ gửi dữ liệu: 1 giây / lần (Đã tắt in log mỗi giây để tối ưu)");
        System.out.println("=========================================================");
    }

    /**
     * TỐI ƯU XỬ LÝ & BĂNG THÔNG:
     * Định kỳ 1 giây gửi dữ liệu tới các Client trong activeClients.
     * Hoàn toàn KHÔNG cần kiểm tra cờ trạng thái hay lọc Stream.
     * ĐÃ TẮT IN LOG CONSOLE MỖI GIÂY để tránh tắc nghẽn I/O và spam màn hình.
     */
    private void startBroadcaster() {
        scheduler.scheduleAtFixedRate(() -> {
            // TỐI ƯU O(1): Nếu không có client nào đang active -> bỏ qua ngay lập tức, không tốn CPU
            if (!isRunning || activeClients.isEmpty()) {
                return;
            }

            try {
                // 1. Sinh dữ liệu ngẫu nhiên
                ExchangeRateData data = ExchangeRateData.generateRandom();

                // 2. TỐI ƯU: Format và chuyển đổi sang byte array 1 LẦN DUY NHẤT
                byte[] payloadBytes = data.toBytes();

                // 3. Tái sử dụng DatagramPacket cho tất cả client trong vòng lặp này
                DatagramPacket packet = new DatagramPacket(payloadBytes, payloadBytes.length);

                // 4. DUYỆT TRỰC TIẾP: Gửi thẳng tới từng active client mà không cần bất kỳ câu lệnh if nào!
                for (ClientSession client : activeClients.values()) {
                    packet.setAddress(client.getAddress());
                    packet.setPort(client.getPort());
                    socket.send(packet);
                }

                // Không in log console mỗi giây theo yêu cầu để tránh spam màn hình và tối ưu CPU/IO

            } catch (Exception e) {
                if (isRunning) {
                    System.err.println("Lỗi trong quá trình gửi tỉ giá: " + e.getMessage());
                }
            }
        }, 1, 1, TimeUnit.SECONDS);
    }

    /**
     * Vòng lặp lắng nghe lệnh điều khiển từ Client
     */
    private void listenForClientCommands() {
        byte[] buffer = new byte[256];
        DatagramPacket receivePacket = new DatagramPacket(buffer, buffer.length);

        while (isRunning) {
            try {
                // Nhận gói tin từ client
                socket.receive(receivePacket);
                String cmd = new String(receivePacket.getData(), 0, receivePacket.getLength(), StandardCharsets.UTF_8).trim();

                InetAddress clientAddr = receivePacket.getAddress();
                int clientPort = receivePacket.getPort();
                String clientId = clientAddr.getHostAddress() + ":" + clientPort;

                handleCommand(cmd, clientId, clientAddr, clientPort);

            } catch (IOException e) {
                if (isRunning) {
                    System.err.println("Lỗi nhận gói tin lệnh từ Client: " + e.getMessage());
                }
            }
        }
    }

    /**
     * Xử lý các lệnh từ client:
     * - SUBSCRIBE / RESUME : Thêm vào activeClients
     * - PAUSE / UNSUBSCRIBE: Xóa khỏi activeClients ngay lập tức (không cần cờ trạng thái)
     */
    private void handleCommand(String cmd, String clientId, InetAddress address, int port) {
        switch (cmd.toUpperCase()) {
            case ExchangeRateData.CMD_SUBSCRIBE:
            case ExchangeRateData.CMD_RESUME:
                activeClients.put(clientId, new ClientSession(address, port));
                System.out.printf("[LỆNH] Client %s -> %s. Số client đang active: %d%n",
                        clientId, cmd.toUpperCase(), activeClients.size());
                break;

            case ExchangeRateData.CMD_PAUSE:
            case ExchangeRateData.CMD_UNSUBSCRIBE:
                ClientSession removed = activeClients.remove(clientId);
                if (removed != null) {
                    System.out.printf("[LỆNH] Client %s -> %s (đã xóa khỏi danh sách gửi tin). Còn lại: %d%n",
                            clientId, cmd.toUpperCase(), activeClients.size());
                }
                break;

            default:
                System.out.printf("[CẢNH BÁO] Nhận lệnh không xác định '%s' từ %s%n", cmd, clientId);
                break;
        }
    }

    public void stop() {
        isRunning = false;
        if (scheduler != null && !scheduler.isShutdown()) {
            scheduler.shutdownNow();
        }
        if (socket != null && !socket.isClosed()) {
            socket.close();
        }
        System.out.println("ExchangeRateServer đã dừng.");
    }

    public static void main(String[] args) {
        int port = ExchangeRateData.DEFAULT_PORT;
        if (args.length > 0) {
            try {
                port = Integer.parseInt(args[0]);
            } catch (NumberFormatException e) {
                System.out.println("Cổng không hợp lệ, dùng cổng mặc định: " + port);
            }
        }

        ExchangeRateServer server = new ExchangeRateServer(port);

        // Đảm bảo đóng socket sạch sẽ khi tắt chương trình (Ctrl+C)
        Runtime.getRuntime().addShutdownHook(new Thread(server::stop));

        server.start();
    }
}
