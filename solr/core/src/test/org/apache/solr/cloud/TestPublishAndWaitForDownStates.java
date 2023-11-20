/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.cloud;

import java.time.Instant;
import java.util.concurrent.TimeUnit;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.junit.After;
import org.junit.Before;

/**
 * Tests the behavior of ZkController when it publishes a DOWNNODE message and waits for all
 * relevant replicas to be published DOWN
 */
public class TestPublishAndWaitForDownStates extends SolrCloudTestCase {

  @Before
  public void configureCluster() throws Exception {
    configureCluster(2).addConfig("conf", configset("cloud-minimal")).configure();
  }

  @After
  @Override
  public void tearDown() throws Exception {
    super.tearDown();
    cluster.deleteAllCollections();
    shutdownCluster();
  }

  /** Verify that the wait on replica states being down is actually observed */
  public void testWaitForDownStates() throws Exception {

    final String collectionName = "testWaitForDownStates";
    CollectionAdminRequest.createCollection(collectionName, "conf", 1, 2)
        .process(cluster.getSolrClient());

    cluster.waitForActiveCollection(collectionName, 1, 2);

    // Wait without sending the DOWNNODE message - it must timeout, because the replica will always
    // be active
    assertTrue("Expected waitForDownStates to time out", waitForDownStates(3));
  }

  /**
   * The same test as testWaitForDownStates, except the DOWNNODE message is published and the wait
   * does not time out
   */
  public void testPublishAndWaitForDownStates() throws Exception {
    final String collectionName = "testPublishAndWaitForDownStates";
    CollectionAdminRequest.createCollection(collectionName, "conf", 1, 2)
        .process(cluster.getSolrClient());

    cluster.waitForActiveCollection(collectionName, 1, 2);

    ZkController controller = cluster.getOpenOverseer().getZkController();
    String nodeName = controller.getNodeName();

    // Remove the ephemeral live node, so that the DOWNNODE message executes fully, rather than exit
    // prematurely
    controller.removeEphemeralLiveNode();
    controller.publishNodeAsDown(nodeName);
    assertFalse(
        "Expected waitForDownStates to observe all replicas as DOWN within the timeout",
        waitForDownStates(3));
  }

  /**
   * Calls {@limk ZkController#waitForDownStates}
   *
   * @return true, if the operation times out, and false if it completes successfully before timing
   *     out
   */
  private boolean waitForDownStates(int timeoutSeconds) throws InterruptedException {
    Instant start = Instant.now();
    cluster.getOpenOverseer().getZkController().waitForDownStates(timeoutSeconds);
    return Instant.now().toEpochMilli() - start.toEpochMilli()
        >= TimeUnit.SECONDS.toMillis(timeoutSeconds);
  }
}
