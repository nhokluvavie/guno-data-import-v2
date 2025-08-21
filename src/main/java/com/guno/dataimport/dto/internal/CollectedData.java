package com.guno.dataimport.dto.internal;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.util.ArrayList;
import java.util.List;

/**
 * Container for collected data from all platforms
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CollectedData {

    @Builder.Default
    private List<Object> shopeeOrders = new ArrayList<>();

    @Builder.Default
    private List<Object> tikTokOrders = new ArrayList<>();

    @Builder.Default
    private List<Object> facebookOrders = new ArrayList<>();

    public int getTotalOrders() {
        return shopeeOrders.size() + tikTokOrders.size() + facebookOrders.size();
    }

    public boolean isEmpty() {
        return getTotalOrders() == 0;
    }
}