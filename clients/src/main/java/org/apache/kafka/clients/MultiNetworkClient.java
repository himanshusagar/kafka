package org.apache.kafka.clients;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.requests.AbstractRequest;

import java.io.IOException;
import java.util.List;

public class MultiNetworkClient implements KafkaClient
{
    /* the state of each nodes connection */
    public KafkaClient leaderClient;
    /* the state of each nodes connection for produce follower requests */
    public KafkaClient followerClient;

    public MultiNetworkClient(KafkaClient leaderClient, KafkaClient followerClient)
    {
        this.leaderClient = leaderClient;
        this.followerClient = followerClient;
    }
    @Override
    public boolean isReady(Node node, long now) {
        return leaderClient.isReady(node, now) && followerClient.isReady(node, now);
    }

    @Override
    public boolean ready(Node node, long now) {
        return leaderClient.ready(node, now) && followerClient.ready(node, now);
    }

    @Override
    public long connectionDelay(Node node, long now) {
        return leaderClient.connectionDelay(node, now) + followerClient.connectionDelay(node, now);
    }

    @Override
    public long pollDelayMs(Node node, long now) {
        return leaderClient.pollDelayMs(node, now) + followerClient.pollDelayMs(node, now);
    }

    @Override
    public boolean connectionFailed(Node node) {
        return leaderClient.connectionFailed(node) && followerClient.connectionFailed(node);
    }

    @Override
    public AuthenticationException authenticationException(Node node) {
        AuthenticationException e = leaderClient.authenticationException(node);
        if(e != null)
            return e;
        return followerClient.authenticationException(node);
    }

    @Override
    public void send(ClientRequest request, long now) {
        leaderClient.send(request , now);
        followerClient.send(request , now);
    }

    @Override
    public List<ClientResponse> poll(long timeout, long now) {
        List<ClientResponse> clientResponsesList = leaderClient.poll(timeout , now);
        List<ClientResponse> clientResponsesList2 = followerClient.poll(timeout , now);
        
        clientResponsesList.addAll(clientResponsesList2);
        return clientResponsesList;
    }

    @Override
    public void disconnect(String nodeId) {
        leaderClient.disconnect(nodeId);
        followerClient.disconnect(nodeId);
    }

    @Override
    public void close(String nodeId) {
        leaderClient.close(nodeId);
        followerClient.close(nodeId);
    }

    @Override
    public Node leastLoadedNode(long now) {
        return leaderClient.leastLoadedNode(now);
    }

    @Override
    public int inFlightRequestCount() {
        return leaderClient.inFlightRequestCount() + followerClient.inFlightRequestCount();
    }

    @Override
    public boolean hasInFlightRequests() {
        return leaderClient.hasInFlightRequests() || followerClient.hasInFlightRequests();
    }

    @Override
    public int inFlightRequestCount(String nodeId) {
        return leaderClient.inFlightRequestCount(nodeId) + followerClient.inFlightRequestCount(nodeId);
    }

    @Override
    public boolean hasInFlightRequests(String nodeId) {
        return leaderClient.hasInFlightRequests() || followerClient.hasInFlightRequests();
    }

    @Override
    public boolean hasReadyNodes(long now) {
        return leaderClient.hasReadyNodes(now) || followerClient.hasReadyNodes(now);
    }

    @Override
    public void wakeup() {
        leaderClient.wakeup();
        followerClient.wakeup();
    }

    @Override
    public ClientRequest newClientRequest(String nodeId, AbstractRequest.Builder<?> requestBuilder, long createdTimeMs, boolean expectResponse) {
        return null;
    }

    @Override
    public ClientRequest newClientRequest(String nodeId, AbstractRequest.Builder<?> requestBuilder, long createdTimeMs, boolean expectResponse, int requestTimeoutMs, RequestCompletionHandler callback) {
        return null;

    }

    @Override
    public void initiateClose() {
        leaderClient.initiateClose();
    }

    @Override
    public boolean active() {
        return leaderClient.active() && followerClient.active();
    }

    @Override
    public void close() throws IOException {
        leaderClient.close();
        followerClient.close();
    }
}
