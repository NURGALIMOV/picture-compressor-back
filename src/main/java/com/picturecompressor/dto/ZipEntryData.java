package com.picturecompressor.dto;

import java.util.Arrays;
import java.util.Objects;

/**
 * Record representing a ZIP entry
 */
public record ZipEntryData(String filename, byte[] data) {
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ZipEntryData that = (ZipEntryData) o;
        return Objects.deepEquals(data, that.data) && Objects.equals(filename, that.filename);
    }

    @Override
    public int hashCode() {
        return Objects.hash(filename, Arrays.hashCode(data));
    }

    @Override
    public String toString() {
        return "ZipEntryData{" +
                "filename='" + filename + '\'' +
                ", data=" + Arrays.toString(data) +
                '}';
    }
}
