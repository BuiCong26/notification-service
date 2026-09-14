package com.lucas.notification.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.mail.internet.MimeMessage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.mail.javamail.MimeMessageHelper;
import org.springframework.stereotype.Service;

@Service
public class EmailListenerService {

    private final JavaMailSender mailSender;
    private final ObjectMapper objectMapper;

    public EmailListenerService(JavaMailSender mailSender, ObjectMapper objectMapper) {
        this.mailSender = mailSender;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "user-register-event", groupId = "notification-group")
    public void handleUserRegisterEvent(String message) {
        try {
            // 1. Phân tích chuỗi JSON từ Kafka nhận được
            JsonNode payload = objectMapper.readTree(message);
            String email = payload.get("email").asText();
            String otp = payload.get("otp").asText();

            //System.out.println("Nhận được yêu cầu gửi OTP đến: " + email);

            // 2. Cấu hình nội dung Email
            MimeMessage mailMessage = mailSender.createMimeMessage();
            // Cờ 'true' cho phép gửi định dạng nâng cao, encoding UTF-8
            MimeMessageHelper helper = new MimeMessageHelper(mailMessage, true, "UTF-8");
            helper.setFrom("congkhoa51@gmail.com", "Lucas Project");
            helper.setTo(email);
            helper.setSubject("Mã xác minh tài khoản Lucas Project");
            helper.setText("Xin chào,\n\nMã xác minh (OTP) của bạn là: " + otp + "\n\nMã này sẽ tự động hết hạn trong vòng 5 phút.");

            // 3. Thực hiện gửi mail
            mailSender.send(mailMessage);
            //System.out.println("Đã gửi Email thành công tới: " + email);

        } catch (Exception e) {
            System.err.println("Lỗi khi xử lý sự kiện gửi mail: " + e.getMessage());
        }
    }
}