package classes;

public class Route {
    private String routeId;
    private String origin;
    private String destination;
    private String transportType;
    private int capacity;
    private String operator;

    // Getters e Setters
    public String getRouteId() { return routeId; }
    public void setRouteId(String routeId) { this.routeId = routeId; }

    public String getOrigin() { return origin; }
    public void setOrigin(String origin) { this.origin = origin; }

    public String getDestination() { return destination; }
    public void setDestination(String destination) { this.destination = destination; }

    public String getTransportType() { return transportType; }
    public void setTransportType(String transportType) { this.transportType = transportType; }

    public int getCapacity() { return capacity; }
    public void setCapacity(int capacity) { this.capacity = capacity; }

    public String getOperator() { return operator; }
    public void setOperator(String operator) { this.operator = operator; }
}
