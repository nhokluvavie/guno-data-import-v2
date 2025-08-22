package com.guno.dataimport.dto.internal;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    public boolean isEmpty() {
        return getTotalOrders() == 0;
    }

    public Map<String, Integer> getPlatformCounts() {
        Map<String, Integer> counts = new HashMap<>();
        counts.put("FACEBOOK", facebookOrders != null ? facebookOrders.size() : 0);
        counts.put("TIKTOK", tikTokOrders != null ? tikTokOrders.size() : 0);
        return counts;
    }

    public int getTotalOrders() {
        int facebook = facebookOrders != null ? facebookOrders.size() : 0;
        int tiktok = tikTokOrders != null ? tikTokOrders.size() : 0;
        return facebook + tiktok;
    }
}