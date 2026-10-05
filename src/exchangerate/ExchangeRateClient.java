package exchangerate;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.Font;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import javax.swing.BorderFactory;
import javax.swing.Box;
import javax.swing.BoxLayout;
import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.JTextField;
import javax.swing.SwingConstants;
import javax.swing.SwingUtilities;
import javax.swing.UIManager;
import javax.swing.table.DefaultTableCellRenderer;
import javax.swing.table.DefaultTableModel;

/**
 * ExchangeRateClient: Client GUI hiển thị tỉ giá nhận từ Server qua UDP Unicast.
 * 
 * =========================================================================
 * CÁC ĐIỂM TỐI ƯU CỦA CLIENT:
 * 1. TỐI ƯU GIAO DIỆN & BỘ NHỚ (GUI & MEMORY):
 *    - JTable đặt trong JScrollPane: Hàng tiêu đề (header) luôn được ghim cố định ở trên cùng.
 *    - Tự động cuộn theo dòng mới nhất (Auto-scroll to bottom): Giúp người xem luôn thấy dữ liệu realtime.
 *    - Giới hạn số dòng tối đa (MAX_ROWS = 50, FIFO): Tự động cắt tỉa hàng cũ nhất (removeRow(0))
 *      khi số dòng vượt quá giới hạn. Tránh tràn RAM và đứng máy nếu mở cả ngày.
 *    - Cung cấp nút "Xóa bảng (Reset)" để người dùng chủ động làm mới giao diện bất kỳ lúc nào.
 * 
 * 2. TỐI ƯU BĂNG THÔNG (BANDWIDTH):
 *    - Khi nhấn Pause: gửi lệnh PAUSE tới Server -> Server ngừng gửi gói tin hoàn toàn.
 *    - Khi đóng ứng dụng: gửi lệnh UNSUBSCRIBE để Server xóa khỏi danh sách, tránh gửi rác.
 * 
 * 3. TỐI ƯU XỬ LÝ (PROCESSING / CPU):
 *    - Luồng nhận mạng chạy ngầm (Background Thread) tách biệt với luồng giao diện (Swing EDT).
 *    - Sử dụng SwingUtilities.invokeLater() đảm bảo thread-safe khi cập nhật JTable.
 *    - Tái sử dụng DatagramPacket và buffer nhận để giảm tải Garbage Collection.
 * =========================================================================
 */
public class ExchangeRateClient extends JFrame {

    private static final int MAX_DISPLAY_ROWS = 50; // Giới hạn hàng tối đa để tối ưu bộ nhớ

    // Swing Components
    private JTextField txtHost;
    private JTextField txtPort;
    private JButton btnConnect;
    private JButton btnPause;
    private JButton btnResume;
    private JButton btnClear;
    private JCheckBox chkAutoScroll;
    private JCheckBox chkLimitRows;
    private JLabel lblStatus;
    private JLabel lblInfo;

    private JTable table;
    private DefaultTableModel tableModel;

    // UDP Network
    private DatagramSocket socket;
    private InetAddress serverAddress;
    private int serverPort;
    private Thread receiveThread;
    private volatile boolean isRunning = false;
    private volatile boolean isPaused = false;
    private long totalReceivedPackets = 0;

    public ExchangeRateClient() {
        super("ExchangeRate Client - UDP Unicast");
        initUI();
    }

    private void initUI() {
        setDefaultCloseOperation(JFrame.DO_NOTHING_ON_CLOSE);
        setSize(780, 560);
        setMinimumSize(new Dimension(650, 400));
        setLocationRelativeTo(null);
        setLayout(new BorderLayout(8, 8));

        // 1. Panel điều khiển phía trên (Top Panel)
        JPanel topPanel = new JPanel();
        topPanel.setLayout(new BoxLayout(topPanel, BoxLayout.Y_AXIS));
        topPanel.setBorder(BorderFactory.createEmptyBorder(10, 12, 6, 12));

        // Dòng cấu hình kết nối & nút bấm
        JPanel connectionPanel = new JPanel(new FlowLayout(FlowLayout.LEFT, 8, 4));
        connectionPanel.add(new JLabel("Server Host:"));
        txtHost = new JTextField(ExchangeRateData.DEFAULT_HOST, 9);
        connectionPanel.add(txtHost);

        connectionPanel.add(new JLabel("Port:"));
        txtPort = new JTextField(String.valueOf(ExchangeRateData.DEFAULT_PORT), 5);
        connectionPanel.add(txtPort);

        btnConnect = new JButton("Kết nối (Start)");
        btnConnect.setFont(new Font("SansSerif", Font.BOLD, 12));
        connectionPanel.add(btnConnect);

        btnPause = new JButton("Tạm dừng (Pause)");
        btnPause.setEnabled(false);
        connectionPanel.add(btnPause);

        btnResume = new JButton("Tiếp tục (Resume)");
        btnResume.setEnabled(false);
        connectionPanel.add(btnResume);

        btnClear = new JButton("Xóa bảng (Reset)");
        connectionPanel.add(btnClear);

        topPanel.add(connectionPanel);

        // Dòng tùy chọn tối ưu & trạng thái
        JPanel optionsPanel = new JPanel(new FlowLayout(FlowLayout.LEFT, 12, 2));
        chkAutoScroll = new JCheckBox("Tự kéo bảng xuống theo (Auto-scroll)", true);
        chkLimitRows = new JCheckBox("Giới hạn " + MAX_DISPLAY_ROWS + " dòng (Tự xóa dòng cũ khi đầy)", true);
        optionsPanel.add(chkAutoScroll);
        optionsPanel.add(chkLimitRows);

        optionsPanel.add(Box.createHorizontalStrut(15));
        optionsPanel.add(new JLabel("Trạng thái:"));
        lblStatus = new JLabel("CHƯA KẾT NỐI");
        lblStatus.setFont(new Font("SansSerif", Font.BOLD, 12));
        lblStatus.setForeground(Color.GRAY);
        optionsPanel.add(lblStatus);

        topPanel.add(optionsPanel);
        add(topPanel, BorderLayout.NORTH);

        // 2. Bảng hiển thị tỉ giá ở trung tâm (Center Table)
        String[] columnNames = {
            "Thời gian (Time)",
            "Tokyo (JPY / VND)",
            "Paris (EUR / VND)",
            "Seoul (KRW / VND)"
        };

        // Table Model: Không cho phép chỉnh sửa nội dung ô
        tableModel = new DefaultTableModel(columnNames, 0) {
            @Override
            public boolean isCellEditable(int row, int column) {
                return false;
            }
        };

        table = new JTable(tableModel);
        table.setRowHeight(26);
        table.setFont(new Font("Monospaced", Font.PLAIN, 13));
        table.getTableHeader().setFont(new Font("SansSerif", Font.BOLD, 13));
        table.getTableHeader().setReorderingAllowed(false); // Không cho kéo đổi vị trí cột

        // Canh lề các cột
        DefaultTableCellRenderer centerRenderer = new DefaultTableCellRenderer();
        centerRenderer.setHorizontalAlignment(SwingConstants.CENTER);
        DefaultTableCellRenderer rightRenderer = new DefaultTableCellRenderer();
        rightRenderer.setHorizontalAlignment(SwingConstants.RIGHT);

        table.getColumnModel().getColumn(0).setCellRenderer(centerRenderer); // Cột Time căn giữa
        table.getColumnModel().getColumn(1).setCellRenderer(rightRenderer);  // Tokyo căn phải
        table.getColumnModel().getColumn(2).setCellRenderer(rightRenderer);  // Paris căn phải
        table.getColumnModel().getColumn(3).setCellRenderer(rightRenderer);  // Seoul căn phải

        // JScrollPane tự động ghim Header ở hàng trên cùng
        JScrollPane scrollPane = new JScrollPane(table);
        scrollPane.setBorder(BorderFactory.createTitledBorder("Bảng theo dõi tỉ giá theo thời gian thực (Cập nhật mỗi 1s)"));
        add(scrollPane, BorderLayout.CENTER);

        // 3. Thanh trạng thái phía dưới (Bottom Status Bar)
        JPanel bottomPanel = new JPanel(new FlowLayout(FlowLayout.LEFT, 12, 6));
        bottomPanel.setBorder(BorderFactory.createEtchedBorder());
        lblInfo = new JLabel("Giao thức: UDP Unicast | Payload siêu nhẹ (~35-45 bytes/gói) | Sẵn sàng kết nối");
        lblInfo.setFont(new Font("SansSerif", Font.PLAIN, 12));
        bottomPanel.add(lblInfo);
        add(bottomPanel, BorderLayout.SOUTH);

        // 4. Đăng ký các sự kiện nút bấm
        btnConnect.addActionListener(e -> connectAndSubscribe());
        btnPause.addActionListener(e -> pauseDataStream());
        btnResume.addActionListener(e -> resumeDataStream());
        btnClear.addActionListener(e -> clearTable());

        // Đóng cửa sổ sạch sẽ: gửi UNSUBSCRIBE tới Server và giải phóng Socket
        addWindowListener(new WindowAdapter() {
            @Override
            public void windowClosing(WindowEvent e) {
                disconnectAndExit();
            }
        });
    }

    /**
     * Kết nối và gửi lệnh SUBSCRIBE tới Server
     */
    private void connectAndSubscribe() {
        try {
            String host = txtHost.getText().trim();
            int port = Integer.parseInt(txtPort.getText().trim());

            serverAddress = InetAddress.getByName(host);
            serverPort = port;

            if (socket == null || socket.isClosed()) {
                socket = new DatagramSocket(); // OS tự cấp phát port ngẫu nhiên
            }

            // Gửi lệnh SUBSCRIBE
            sendCommand(ExchangeRateData.CMD_SUBSCRIBE);

            isRunning = true;
            isPaused = false;

            btnConnect.setEnabled(false);
            txtHost.setEditable(false);
            txtPort.setEditable(false);
            btnPause.setEnabled(true);
            btnResume.setEnabled(false);

            lblStatus.setText("ĐANG NHẬN DỮ LIỆU");
            lblStatus.setForeground(new Color(0, 150, 0)); // Màu xanh lá

            // Khởi động luồng nhận ngầm
            startReceiverThread();

        } catch (Exception ex) {
            JOptionPane.showMessageDialog(this, "Không thể kết nối Server: " + ex.getMessage(),
                    "Lỗi kết nối", JOptionPane.ERROR_MESSAGE);
        }
    }

    /**
     * TỐI ƯU BĂNG THÔNG: Gửi lệnh PAUSE để Server ngừng gửi gói tin
     */
    private void pauseDataStream() {
        if (!isRunning || isPaused) return;

        sendCommand(ExchangeRateData.CMD_PAUSE);
        isPaused = true;
        btnPause.setEnabled(false);
        btnResume.setEnabled(true);

        lblStatus.setText("ĐÃ TẠM DỪNG (PAUSED)");
        lblStatus.setForeground(new Color(200, 120, 0)); // Màu cam

        lblInfo.setText("Đã tạm dừng nhận");
    }

    /**
     * Gửi lệnh RESUME để Server tiếp tục gửi tỉ giá mỗi giây
     */
    private void resumeDataStream() {
        if (!isRunning || !isPaused) return;

        sendCommand(ExchangeRateData.CMD_RESUME);
        isPaused = false;
        btnPause.setEnabled(true);
        btnResume.setEnabled(false);

        lblStatus.setText("ĐANG NHẬN DỮ LIỆU");
        lblStatus.setForeground(new Color(0, 150, 0)); // Màu xanh lá
    }

    /**
     * Xóa sạch dữ liệu bảng (Reset)
     */
    private void clearTable() {
        tableModel.setRowCount(0);
        lblInfo.setText("Đã xóa bảng dữ liệu.");
    }

    /**
     * Gửi lệnh UDP dạng chuỗi tới Server
     */
    private void sendCommand(String cmd) {
        if (socket == null || socket.isClosed() || serverAddress == null) {
            return;
        }
        try {
            byte[] bytes = cmd.getBytes(StandardCharsets.UTF_8);
            DatagramPacket packet = new DatagramPacket(bytes, bytes.length, serverAddress, serverPort);
            socket.send(packet);
        } catch (IOException e) {
            System.err.println("Lỗi gửi lệnh tới server: " + e.getMessage());
        }
    }

    /**
     * TỐI ƯU XỬ LÝ: Luồng nhận UDP chạy nền, không làm đơ giao diện Swing
     */
    private void startReceiverThread() {
        if (receiveThread != null && receiveThread.isAlive()) {
            return;
        }

        receiveThread = new Thread(() -> {
            byte[] buffer = new byte[256]; // Kích thước đủ lớn cho chuỗi CSV ngắn
            DatagramPacket packet = new DatagramPacket(buffer, buffer.length);

            while (isRunning) {
                try {
                    socket.receive(packet);
                    totalReceivedPackets++;

                    String payload = new String(packet.getData(), 0, packet.getLength(), StandardCharsets.UTF_8);
                    int packetSize = packet.getLength();

                    ExchangeRateData data = ExchangeRateData.deserialize(payload);
                    if (data != null) {
                        // TỐI ƯU SWING: Đưa thao tác update UI vào Event Dispatch Thread (EDT)
                        SwingUtilities.invokeLater(() -> addRowToTable(data, packetSize));
                    }

                } catch (IOException e) {
                    if (isRunning) {
                        System.err.println("Socket nhận gặp lỗi: " + e.getMessage());
                    }
                }
            }
        }, "UDP-Receiver-Thread");

        receiveThread.setDaemon(true);
        receiveThread.start();
    }

    /**
     * Thêm dòng dữ liệu mới vào bảng và thực hiện các tối ưu giao diện:
     * - Tự xóa dòng cũ khi đầy (giới hạn MAX_DISPLAY_ROWS dòng).
     * - Tự kéo bảng xuống theo (Auto-scroll).
     */
    private void addRowToTable(ExchangeRateData data, int packetBytes) {
        // Format hiển thị số liệu đẹp mắt theo VNĐ
        Object[] rowData = {
            data.getTime(),
            String.format(java.util.Locale.US, "%,.2f", data.getTokyoRate()),
            String.format(java.util.Locale.US, "%,.2f", data.getParisRate()),
            String.format(java.util.Locale.US, "%,.2f", data.getSeoulRate())
        };

        tableModel.addRow(rowData);

        // TỐI ƯU BỘ NHỚ: Nếu vượt quá MAX_ROWS và có bật checkbox giới hạn -> xóa dòng cũ nhất
        if (chkLimitRows.isSelected() && tableModel.getRowCount() > MAX_DISPLAY_ROWS) {
            tableModel.removeRow(0); // Xóa dòng đầu tiên (FIFO)
        }

        // TỐI ƯU TRẢI NGHIỆM: Tự động cuộn xuống dòng mới nhất vừa thêm
        if (chkAutoScroll.isSelected()) {
            int lastRowIndex = tableModel.getRowCount() - 1;
            if (lastRowIndex >= 0) {
                table.scrollRectToVisible(table.getCellRect(lastRowIndex, 0, true));
            }
        }

        // Cập nhật thanh trạng thái phía dưới
        lblInfo.setText(String.format("Gói tin mới nhất lúc %s | Size: %d bytes | Tổng gói nhận: %d | Số dòng hiển thị: %d",
                data.getTime(), packetBytes, totalReceivedPackets, tableModel.getRowCount()));
    }

    /**
     * Ngắt kết nối và giải phóng tài nguyên khi tắt ứng dụng
     */
    private void disconnectAndExit() {
        if (isRunning) {
            sendCommand(ExchangeRateData.CMD_UNSUBSCRIBE);
            isRunning = false;
        }
        if (socket != null && !socket.isClosed()) {
            socket.close();
        }
        dispose();
        System.exit(0);
    }

    public static void main(String[] args) {
        // Thiết lập giao diện theo hệ điều hành (Look & Feel)
        try {
            UIManager.setLookAndFeel(UIManager.getSystemLookAndFeelClassName());
        } catch (Exception ignored) {
        }

        SwingUtilities.invokeLater(() -> {
            ExchangeRateClient client = new ExchangeRateClient();
            client.setVisible(true);
        });
    }
}
