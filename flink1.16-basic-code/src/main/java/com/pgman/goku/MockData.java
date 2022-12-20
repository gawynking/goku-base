package com.pgman.goku;

import com.goku.util.MockDataUtils;

public class MockData {
    public static void main(String[] args) {
        MockDataUtils.mockOrderStreamData(false, 3000, 3);
    }
}
