package com.dnclabs.banking.account;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Account Service — quản tài khoản ví & số dư (balance).
 *
 * <p>Bounded context "Account" trong hệ Banking/Wallet (Bài 0.5):
 * giữ số dư trực tiếp, thực hiện debit/credit, và (từ Module 6) phát event
 * qua Outbox để Ledger/Notification lắng nghe.
 */
@SpringBootApplication
public class AccountServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(AccountServiceApplication.class, args);
    }
}
