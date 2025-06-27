package com.picturecompressor.dto;

import java.util.Arrays;
import java.util.Objects;

/**
 * Record to hold file data for batch processing
 */
public record FileData(String filename, byte[] data) {
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FileData fileData = (FileData) o;
        return Objects.equals(filename, fileData.filename) &&
                Arrays.equals(data, fileData.data);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(filename);
        result = 31 * result + Arrays.hashCode(data);
        return result;
    }

    @Override
    public String toString() {
        return "FileData{" +
                "filename='" + filename + '\'' +
                ", data=" + Arrays.toString(data) +
                '}';
    }
}