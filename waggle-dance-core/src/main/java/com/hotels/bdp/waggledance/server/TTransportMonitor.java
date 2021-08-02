/**
 * Copyright (C) 2016-2021 Expedia, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hotels.bdp.waggledance.server;

import java.io.Closeable;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;

import javax.annotation.PreDestroy;
import javax.annotation.WillClose;

import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;

import com.hotels.bdp.waggledance.conf.WaggleDanceConfiguration;

@Component
public class TTransportMonitor {

  private static final Logger LOG = LoggerFactory.getLogger(TTransportMonitor.class);

  private static class ActionContainer {
    private final TTransport transport;
    private final Closeable action;

    private ActionContainer(TTransport transport, Closeable action) {
      this.transport = transport;
      this.action = action;
    }
  }

  private final ScheduledExecutorService monitorScheduler;

  private final ScheduledExecutorService cleanerScheduler;
  private final List<ActionContainer> todoActionContainerList = Lists.newLinkedList();
  private final LinkedBlockingQueue<ActionContainer> addonQueue = new LinkedBlockingQueue<>();
  private final LinkedBlockingQueue<ActionContainer> toCloseQueue = new LinkedBlockingQueue<>();

  @Autowired
  public TTransportMonitor(WaggleDanceConfiguration waggleDanceConfiguration) {
    this(waggleDanceConfiguration,
      Executors.newScheduledThreadPool(1),
      Executors.newScheduledThreadPool(waggleDanceConfiguration.getDisconnectConnectionThreads()));
  }

  @VisibleForTesting
  TTransportMonitor(WaggleDanceConfiguration waggleDanceConfiguration,
                    ScheduledExecutorService monitorScheduler,
                    ScheduledExecutorService cleanerScheduler) {
    this.monitorScheduler = monitorScheduler;
    this.cleanerScheduler = cleanerScheduler;
    Runnable monitor = () -> {
      LOG.debug("Checking disconnected sessions");
      ActionContainer actionContainer;
      try {
        // Traversal the todoActionContainerList within a single thread to avoid the concurrent problem.
        Iterator<ActionContainer> iterator = todoActionContainerList.iterator();
        while (iterator.hasNext()) {
          actionContainer = iterator.next();
          // Move those closed to the toDestroyQueue
          if (!actionContainer.transport.peek()) {
            toCloseQueue.add(actionContainer);
            iterator.remove();
          }
        }

        while ((actionContainer = addonQueue.poll()) != null) {
          if (actionContainer.transport.peek()) {
            todoActionContainerList.add(actionContainer);
          } else {
            toCloseQueue.add(actionContainer);
          }
        }
      } catch (Exception e) {
        LOG.error("Error checking the disconnected sessions.");
      } finally {
        LOG.debug("Size of remaining session to check is " + todoActionContainerList.size() +
          ", size of session to clean is " + toCloseQueue.size());
      }
    };
    this.monitorScheduler
        .scheduleAtFixedRate(monitor, waggleDanceConfiguration.getDisconnectConnectionDelay(),
            waggleDanceConfiguration.getDisconnectConnectionDelay(), waggleDanceConfiguration.getDisconnectTimeUnit());

    // Clean the closed transport and its underlying resources.
    for (int i = 0; i < waggleDanceConfiguration.getDisconnectConnectionThreads(); i++) {
      Runnable cleaner = () -> {
        ActionContainer actionContainer = null;
        while ((actionContainer = toCloseQueue.poll()) != null) {
          try {
            actionContainer.transport.close();
          } catch (Exception e) {
            LOG.warn("Error closing transport", e);
          }

          try {
            actionContainer.action.close();
          } catch (Exception e) {
            LOG.warn("Error closing action", e);
          }
          int queueSize = toCloseQueue.size();
          if (queueSize != 0 && queueSize % 1000 == 0) {
            LOG.debug("Remaining sessions to be cleaned count " + queueSize);
          }
        }
      };

      this.cleanerScheduler.scheduleAtFixedRate(cleaner, waggleDanceConfiguration.getDisconnectConnectionDelay(), waggleDanceConfiguration.getDisconnectConnectionDelay(),
        waggleDanceConfiguration.getDisconnectTimeUnit());
    }
  }

  @PreDestroy
  public void shutdown() {
    monitorScheduler.shutdown();
    cleanerScheduler.shutdown();
  }

  public void monitor(@WillClose TTransport transport, @WillClose Closeable action) {
    addonQueue.offer(new ActionContainer(transport, action));
  }

}
