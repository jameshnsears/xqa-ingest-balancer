package xqa;

import java.util.Date;

class IngestBalancerEvent {
    private final String serviceId;
    private final long creationTime;
    private final String correlationId;
    private final int poolSize;
    private final String digest;
    private final String state;

    public IngestBalancerEvent(final String serviceId,
                               final String correlationId,
                               final int poolSize,
                               final String digest,
                               final String state) {
        this.serviceId = serviceId;
        this.creationTime = new Date().getTime();
        this.correlationId = correlationId;
        this.poolSize = poolSize;
        this.digest = digest;
        this.state = state;
    }
}
