package com.jpmc.midascore.component;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.repository.TransactionRecordRepository;
import com.jpmc.midascore.repository.UserRepository;
import jakarta.transaction.Transactional;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;


@Component
public class KafkaConsumer {

    private final UserRepository userRepository;
    private final TransactionRecordRepository transactionRecordRepository;
    private final DatabaseConduit databaseConduit;

    public KafkaConsumer(
            UserRepository userRepository,
            TransactionRecordRepository transactionRecordRepository,
            DatabaseConduit databaseConduit
    ) {
        this.userRepository = userRepository;
        this.transactionRecordRepository = transactionRecordRepository;
        this.databaseConduit = databaseConduit;
    }

    @KafkaListener(topics = "${general.kafka-topic}")
    @Transactional
    public void listen(Transaction transaction) {

        // 1. Validate sender and recipient
        UserRecord sender = userRepository.findById(transaction.getSenderId());
        UserRecord recipient = userRepository.findById(transaction.getRecipientId());

        if (sender == null || recipient == null) {
            return;
        }

        // 2. Validate sufficient balance
        if (sender.getBalance() < transaction.getAmount()) {
            return;
        }

        // 3. Adjust balances
        sender.setBalance(sender.getBalance() - transaction.getAmount());
        recipient.setBalance(recipient.getBalance() + transaction.getAmount());

        // 4. Persist updated users via DatabaseConduit
        databaseConduit.save(sender);
        databaseConduit.save(recipient);

        // 5. Persist transaction record
        TransactionRecord record =
                new TransactionRecord(sender, recipient, transaction.getAmount());

        transactionRecordRepository.save(record);
    }
}
