package io.velo;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

/**
 * Immutable host:port pair. The single value type for network endpoints throughout Velo,
 * replacing the former {@code ReplPair.HostAndPort} record and the
 * {@code io.velo.repl.cluster.watch.HostAndPort} Groovy class.
 *
 * <p>Fields are public and final; access them directly ({@code hp.host}, {@code hp.port}).
 * {@link #toString()} renders the {@code host:port} form (not the auto-generated record form).
 */
public final class HostAndPort {
    public final String host;
    public final int port;

    @JsonCreator
    public HostAndPort(@JsonProperty("host") String host, @JsonProperty("port") int port) {
        this.host = host;
        this.port = port;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof HostAndPort that)) return false;
        return port == that.port && Objects.equals(host, that.host);
    }

    @Override
    public int hashCode() {
        return Objects.hash(host, port);
    }

    @Override
    public String toString() {
        return host + ":" + port;
    }

    /**
     * Parses a strict {@code host:port} string (IPv4 literal or DNS name, exactly one colon,
     * port in 1..65535). Throws on any malformed input.
     *
     * @param hostAndPort the host:port string to parse
     * @return the HostAndPort, or {@code null} if the input is {@code null}
     * @throws IllegalArgumentException if the format is invalid
     */
    public static HostAndPort parse(String hostAndPort) {
        if (hostAndPort == null) {
            return null;
        }

        var separatorIndex = hostAndPort.indexOf(':');
        if (separatorIndex <= 0 || separatorIndex != hostAndPort.lastIndexOf(':')
                || separatorIndex == hostAndPort.length() - 1) {
            throw new IllegalArgumentException("Invalid host:port format: " + hostAndPort);
        }

        var host = hostAndPort.substring(0, separatorIndex);
        try {
            var port = Integer.parseInt(hostAndPort.substring(separatorIndex + 1));
            if (port <= 0 || port > 65535) {
                throw new IllegalArgumentException("Invalid host:port format: " + hostAndPort);
            }
            return new HostAndPort(host, port);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid host:port format: " + hostAndPort, e);
        }
    }
}
