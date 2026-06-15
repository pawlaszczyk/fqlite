package digital.codespiresolutions.nodeql;

public record Position(double x, double y) {
    public static Position zero() {
        return new Position(0, 0);
    }
}
