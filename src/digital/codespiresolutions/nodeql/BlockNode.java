package digital.codespiresolutions.nodeql;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public final class BlockNode {
    private final String id;
    private final BlockType type;
    private final Position position;
    private final BlockNode parent;
    private final List<BlockNode> children;
    private final Map<String, Object> inputs;
    private BlockNode next;

    public BlockNode(String id, BlockType type) {
        this(id, type, Position.zero(), null, List.of(), Map.of());
    }

    public BlockNode(
            String id,
            BlockType type,
            Position position,
            BlockNode parent,
            List<BlockNode> children,
            Map<String, ?> inputs) {
        this.id = Objects.requireNonNull(id, "id");
        this.type = Objects.requireNonNull(type, "type");
        this.position = position == null ? Position.zero() : position;
        this.parent = parent;
        this.children = new ArrayList<>(children == null ? List.of() : children);
        this.inputs = new LinkedHashMap<>();
        if (inputs != null) {
            this.inputs.putAll(inputs);
        }
    }

    public String id() {
        return id;
    }

    public BlockType type() {
        return type;
    }

    public Position position() {
        return position;
    }

    public BlockNode parent() {
        return parent;
    }

    public List<BlockNode> children() {
        return children;
    }

    public Map<String, Object> inputs() {
        return inputs;
    }

    public BlockNode next() {
        return next;
    }

    public void setNext(BlockNode next) {
        this.next = next;
    }
}
