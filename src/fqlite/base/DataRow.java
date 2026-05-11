package fqlite.base;

import java.util.LinkedList;

public record DataRow(
    LinkedList<String> line,
    LinkedList<byte[]> hexdump
){}

