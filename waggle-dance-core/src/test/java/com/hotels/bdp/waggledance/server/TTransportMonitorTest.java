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

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.Closeable;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.thrift.transport.TTransport;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.hotels.bdp.waggledance.conf.WaggleDanceConfiguration;

@RunWith(MockitoJUnitRunner.class)
public class TTransportMonitorTest {

  private static final long DEFAULT_DELAY = 5;

  private final ArgumentCaptor<Runnable> runnableCaptor = ArgumentCaptor.forClass(Runnable.class);

  private @Mock WaggleDanceConfiguration waggleDanceConfiguration;
  private @Mock TTransport transport;
  private @Mock Closeable action;
  private @Mock ScheduledExecutorService monitorScheduler;
  private @Mock ScheduledExecutorService cleanerScheduler;

  private TTransportMonitor monitor;

  @Before
  public void init() {
    when(waggleDanceConfiguration.getDisconnectConnectionDelay()).thenReturn((int) DEFAULT_DELAY);
    when(waggleDanceConfiguration.getDisconnectTimeUnit()).thenReturn(MILLISECONDS);
    when(waggleDanceConfiguration.getDisconnectConnectionThreads()).thenReturn(1);
    monitor = new TTransportMonitor(waggleDanceConfiguration, monitorScheduler, cleanerScheduler);
    verify(monitorScheduler).scheduleAtFixedRate(runnableCaptor.capture(), anyLong(), anyLong(), any(TimeUnit.class));
    verify(cleanerScheduler).scheduleAtFixedRate(runnableCaptor.capture(), anyLong(), anyLong(), any(TimeUnit.class));
  }

  @Test
  public void initialization() throws Exception {
    List<Runnable> runnableList = runnableCaptor.getAllValues();
    assertThat(runnableList, is(notNullValue()));
    verify(monitorScheduler).scheduleAtFixedRate(runnableList.get(0), DEFAULT_DELAY, DEFAULT_DELAY, MILLISECONDS);
    verify(cleanerScheduler).scheduleAtFixedRate(runnableList.get(1), DEFAULT_DELAY, DEFAULT_DELAY, MILLISECONDS);
  }

  @Test
  public void shouldNotDisconnect() throws Exception {
    when(transport.peek()).thenReturn(true);
    monitor.monitor(transport, action);
    for (Runnable runnable : runnableCaptor.getAllValues()) {
        runnable.run();
    }
    verify(transport, never()).close();
    verify(action, never()).close();
  }

  @Test
  public void shouldDisconnect() throws Exception {
    when(transport.peek()).thenReturn(false);
    monitor.monitor(transport, action);
    for (Runnable runnable : runnableCaptor.getAllValues()) {
      runnable.run();
    }
    verify(transport).close();
    verify(action).close();
  }

  @Test
  public void shouldDisconnectWhenTransportThrowsException() throws Exception {
    when(transport.peek()).thenReturn(false);
    doThrow(new RuntimeException()).when(transport).close();
    monitor.monitor(transport, action);
    for (Runnable runnable : runnableCaptor.getAllValues()) {
      runnable.run();
    }
    verify(transport).close();
    verify(action).close();
  }

  @Test
  public void shouldDisconnectWhenActionThrowsException() throws Exception {
    when(transport.peek()).thenReturn(false);
    doThrow(new RuntimeException()).when(action).close();
    monitor.monitor(transport, action);
    for (Runnable runnable : runnableCaptor.getAllValues()) {
      runnable.run();
    }
    verify(transport).close();
    verify(action).close();
  }

}
