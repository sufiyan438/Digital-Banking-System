package com.banking.paymentservice.service;

import com.banking.paymentservice.dto.CreatePaymentRequest;
import com.banking.paymentservice.dto.PaymentOrderResponse;
import com.banking.paymentservice.model.Payment;
import com.banking.paymentservice.model.PaymentStatus;
import com.banking.paymentservice.repository.PaymentRepository;
import com.razorpay.Order;
import com.razorpay.RazorpayClient;
import com.razorpay.RazorpayException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Service
@Slf4j
@RequiredArgsConstructor
public class PaymentService {
    private final PaymentRepository paymentRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    private static final String PAYMENT_COMPLETED_TOPIC = "payment.completed";
    private static final String PAYMENT_FAILED_TOPIC = "payment.failed";

    @Value("${razorpay.key-id}")
    private String keyId;

    @Value("${razorpay.key-secret}")
    private String keySecret;

    public PaymentOrderResponse createPaymentOrder(CreatePaymentRequest request)
        throws RazorpayException {
        log.info("Creating payment order for account: {} amount: {}", request.getAccountNumber(),
                request.getAmount());
        RazorpayClient razorpay = new RazorpayClient(keyId, keySecret);
        int amountInPaise = request.getAmount().multiply(BigDecimal.valueOf(100)).intValue();

        JSONObject orderRequest = new JSONObject();
        orderRequest.put("amount", amountInPaise);
        orderRequest.put("currency", "INR");
        orderRequest.put("receipt", "rcpt_" + UUID.randomUUID().toString()
                .replace("-", "").substring(0, 30));
        Order razorpayOrder = razorpay.orders.create(orderRequest);
        log.info("Razorpay order created. Order ID: {}", razorpayOrder.get("id").toString());

        Payment payment = Payment.builder()
                .razorpayOrderId(razorpayOrder.get("id").toString())
                .accountNumber(request.getAccountNumber())
                .amount(request.getAmount())
                .currency("INR")
                .status(PaymentStatus.CREATED)
                .description(request.getDescription())
                .build();

        Payment saved = paymentRepository.save(payment);
        PaymentOrderResponse response = PaymentOrderResponse.builder()
                .paymentId(saved.getId())
                .razorpayOrderId(razorpayOrder.get("id").toString())
                .razorpayKeyId(keyId)
                .amount(request.getAmount())
                .currency("INR")
                .status("CREATED")
                .build();
        return response;
    }

    public void handleWebhook(Map<String, Object> payload){
        log.info("Received Razorpay webhook: {}", payload.get("event"));
        String event = (String) payload.get("event");
        if(event.equals("payment.captured")){
            handlePaymentSuccess(payload);
        }else if(event.equals("payment.failed")){
            handlePaymentFailure(payload);
        }
    }

    public void handlePaymentSuccess(Map<String, Object> payload){
        try{
            Map<String, Object> paymentData = extractPaymentData(payload);
            String orderId = (String) paymentData.get("order_id");
            String paymentId = (String) paymentData.get("id");
            Payment payment = paymentRepository.findByRazorpayOrderId(orderId)
                    .orElseThrow(() -> new RuntimeException("Payment not found for orderId: " + orderId));
            payment.setRazorpayPaymentId(paymentId);
            payment.setStatus(PaymentStatus.COMPLETED);
            paymentRepository.save(payment);

            Map<String, Object> event = new HashMap<>();
            event.put("paymentId", payment.getId());
            event.put("accountNumber", payment.getAccountNumber());
            event.put("amount", payment.getAmount());
            event.put("razorpayPaymentId", paymentId);

            kafkaTemplate.send(PAYMENT_COMPLETED_TOPIC, payment.getId(), event);
            log.info("Payment {} completed", paymentId);
        }
        catch (Exception e){
            log.error("Error handling payment success: {}", e.getMessage());
        }
    }

    public void handlePaymentFailure(Map<String, Object> payload){
        try{
            Map<String, Object> paymentData = extractPaymentData(payload);
            String orderId = (String) paymentData.get("order_id");
            Payment payment = paymentRepository.findByRazorpayOrderId(orderId)
                    .orElseThrow(() -> new RuntimeException("Payment not found for orderID: " + orderId));

            payment.setStatus(PaymentStatus.FAILED);
            String errorDescription = (String) paymentData.get("error_description");
            payment.setFailureReason(errorDescription == null ? "Payment failed via Razorpay" : errorDescription);
            paymentRepository.save(payment);

            Map<String, Object> event = new HashMap<>();
            event.put("paymentId", payment.getId());
            event.put("accountNumber", payment.getAccountNumber());
            event.put("amount", payment.getAmount());
            event.put("reason", payment.getFailureReason());
            kafkaTemplate.send(PAYMENT_FAILED_TOPIC, payment.getId(), event);
            log.warn("Payment {} failed", payment.getId());
        }
        catch (Exception e){
            log.error("Error catching payment failure: {}", e.getMessage());
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> extractPaymentData(Map<String, Object> payload){
        Map<String, Object> entity = (Map<String, Object>) payload.get("payload");
        Map<String, Object> paymentWrapper = (Map<String, Object>) entity.get("payment");
        return (Map<String, Object>) paymentWrapper.get("entity");
    }
}
