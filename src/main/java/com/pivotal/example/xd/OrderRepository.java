package com.pivotal.example.xd;

import org.springframework.data.repository.CrudRepository;

public interface OrderRepository extends CrudRepository<Order, String> {

    Order findByTransactionId(String transactionId);

    Iterable<Order> findByAmountGreaterThan(double amount);

    Iterable<Order> findByAmountLessThan(double amount);

    Iterable<Order> findByAmountGreaterThanAndAmountLessThan(double amount1, double amount2);

}
