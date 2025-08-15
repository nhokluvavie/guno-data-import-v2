package com.guno.dataimport.buffer;

import com.guno.dataimport.dto.platform.facebook.FacebookOrderDto;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import java.util.ArrayList;
import java.util.List;

/**
 * Memory Buffer - Accumulate orders before bulk processing
 * Optimizes DB operations: N pages → 1 bulk operation
 */
@Slf4j
public class MemoryBuffer {

    @Getter private final int capacity;
    private final List<FacebookOrderDto> orders = new ArrayList<>();

    public MemoryBuffer(int capacity) {
        this.capacity = capacity;
    }

    public void addOrder(FacebookOrderDto order) {
        orders.add(order);
        log.debug("Buffer: {}/{}", orders.size(), capacity);
    }

    public void addOrders(List<FacebookOrderDto> orderList) {
        orders.addAll(orderList);
        log.debug("Buffer: {}/{} (+{})", orders.size(), capacity, orderList.size());
    }

    public boolean isFull() {
        return orders.size() >= capacity;
    }

    public boolean isEmpty() {
        return orders.isEmpty();
    }

    public int size() {
        return orders.size();
    }

    public List<FacebookOrderDto> getOrders() {
        return new ArrayList<>(orders);
    }

    public void clear() {
        int size = orders.size();
        orders.clear();
        log.debug("Buffer cleared: {} orders", size);
    }

    public double getUtilization() {
        return (double) orders.size() / capacity * 100;
    }
}