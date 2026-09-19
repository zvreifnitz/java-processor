package com.github.zvreifnitz.processor.impl.utils;

public record ExecutorInfo(boolean recursionSafe, boolean virtualThread) {
    public ExecutorInfo() {
        this(false, false);
    }
}
