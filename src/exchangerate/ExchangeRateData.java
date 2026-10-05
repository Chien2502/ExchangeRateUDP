package exchangerate;

import java.nio.charset.StandardCharsets;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Lớp biểu diễn dữ liệu tỉ giá và các hàm tiện ích serialize / deserialize.
 * 
 * =========================================================================
 * CÁC ĐIỂM TỐI ƯU:
 * 1. TỐI ƯU BĂNG THÔNG:
 *    - Sử dụng định dạng chuỗi CSV phân tách bằng dấu chấm phẩy (;)
 *      thay vì Java Object Serialization (tiết kiệm ~80% kích thước gói tin).
 *    - Kích thước payload chỉ khoảng ~35 đến 45 bytes/gói tin UDP.
 * 
 * 2. TỐI ƯU LƯU TRỮ & XỬ LÝ:
 *    - Sử dụng kiểu dữ liệu nguyên thủy double, mảng cố định không dùng Map.
 *    - Sinh số ngẫu nhiên bằng ThreadLocalRandom hiệu năng cao.
 * =========================================================================
 */
public class ExchangeRateData {

    // Cổng mặc định giao tiếp UDP
    public static final int DEFAULT_PORT = 9876;
    public static final String DEFAULT_HOST = "localhost";

    // Các lệnh điều khiển từ Client tới Server
    public static final String CMD_SUBSCRIBE = "SUBSCRIBE";
    public static final String CMD_PAUSE = "PAUSE";
    public static final String CMD_RESUME = "RESUME";
    public static final String CMD_UNSUBSCRIBE = "UNSUBSCRIBE";

    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss");

    private String time;
    private double tokyoRate;  // Tỉ giá thị trường Tokyo (JPY / VND)
    private double parisRate;  // Tỉ giá thị trường Paris (EUR / VND)
    private double seoulRate;  // Tỉ giá thị trường Seoul (KRW / VND)

    public ExchangeRateData(String time, double tokyoRate, double parisRate, double seoulRate) {
        this.time = time;
        this.tokyoRate = tokyoRate;
        this.parisRate = parisRate;
        this.seoulRate = seoulRate;
    }

    public String getTime() {
        return time;
    }

    public double getTokyoRate() {
        return tokyoRate;
    }

    public double getParisRate() {
        return parisRate;
    }

    public double getSeoulRate() {
        return seoulRate;
    }

    /**
     * Sinh dữ liệu ngẫu nhiên cho 3 thị trường Tokyo, Paris, Seoul quy đổi theo VNĐ.
     */
    public static ExchangeRateData generateRandom() {
        String currentTime = LocalTime.now().format(TIME_FORMATTER);
        // Tokyo: 1 JPY dao động khoảng 160.00 - 175.00 VND
        double tokyo = 160.0 + ThreadLocalRandom.current().nextDouble() * 15.0;
        // Paris: 1 EUR dao động khoảng 26,500.00 - 27,800.00 VND
        double paris = 26500.0 + ThreadLocalRandom.current().nextDouble() * 1300.0;
        // Seoul: 1 KRW dao động khoảng 17.50 - 19.50 VND
        double seoul = 17.5 + ThreadLocalRandom.current().nextDouble() * 2.0;

        return new ExchangeRateData(currentTime, tokyo, paris, seoul);
    }

    /**
     * TỐI ƯU BĂNG THÔNG:
     * Serialize dữ liệu thành chuỗi rút gọn dạng CSV:
     * Format: "HH:mm:ss;tokyo;paris;seoul"
     * Ví dụ: "07:55:02;165.25;27150.80;18.45"
     */
    public String serialize() {
        return String.format(java.util.Locale.US, "%s;%.2f;%.2f;%.2f", time, tokyoRate, parisRate, seoulRate);
    }

    /**
     * Chuyển chuỗi sang byte[] UTF-8 dùng cho DatagramPacket
     */
    public byte[] toBytes() {
        return serialize().getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Deserialize chuỗi payload nhận từ UDP thành đối tượng ExchangeRateData.
     */
    public static ExchangeRateData deserialize(String payload) {
        if (payload == null || payload.isBlank()) {
            return null;
        }
        String[] parts = payload.trim().split(";");
        if (parts.length < 4) {
            return null;
        }
        String timeStr = parts[0];
        double tokyo = Double.parseDouble(parts[1]);
        double paris = Double.parseDouble(parts[2]);
        double seoul = Double.parseDouble(parts[3]);
        return new ExchangeRateData(timeStr, tokyo, paris, seoul);
    }

    @Override
    public String toString() {
        return String.format(java.util.Locale.US, "[%s] Tokyo (JPY): %.2f VND | Paris (EUR): %.2f VND | Seoul (KRW): %.2f VND",
                time, tokyoRate, parisRate, seoulRate);
    }
}
