/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by
 * applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package org.apache.phoenix.hbase.index.write;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.phoenix.hbase.index.exception.SingleIndexWriteFailureException;
import org.apache.phoenix.hbase.index.table.CachingHTableFactory;
import org.apache.phoenix.hbase.index.table.HTableFactory;
import org.apache.phoenix.hbase.index.table.HTableInterfaceReference;
import org.apache.phoenix.hbase.index.util.KeyValueBuilder;
import org.apache.phoenix.util.IndexUtil;

import com.google.common.collect.Multimap;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.RegionTooBusyException;
import org.apache.hadoop.hbase.client.Table;

/**
 * Update the indexes by the origin thread rather than thread pool.
 */
public class SimpleWriterIndexCommitter implements IndexCommitter {

  private static final Log LOG = LogFactory.getLog(SimpleWriterIndexCommitter.class);
  private HTableFactory factory;
  private Stoppable stopped;
  private KeyValueBuilder kvBuilder;
  private RegionCoprocessorEnvironment env;

  public SimpleWriterIndexCommitter() {
  }

  @VisibleForTesting
  public SimpleWriterIndexCommitter(String hbaseVersion) {
    kvBuilder = KeyValueBuilder.get(hbaseVersion);
  }

  @Override
  public void setup(IndexWriter parent, RegionCoprocessorEnvironment env, String name) {
    this.env = env;
    setup(IndexWriterUtils.getDefaultDelegateHTableFactory(env),
        env.getRegionServerServices(), parent,
        CachingHTableFactory.getCacheSize(env.getConfiguration()), env);
    this.kvBuilder = KeyValueBuilder.get(env.getHBaseVersion());
  }

  @VisibleForTesting
  void setup(HTableFactory factory, Abortable abortable, Stoppable stop, int cacheSize, RegionCoprocessorEnvironment env) {
      this.factory = new CachingHTableFactory(factory, cacheSize, env);
      this.stopped = stop;
  }

  private void updateIndexes(HTableInterfaceReference tableReference,
    List<Mutation> mutations, boolean allowLocalUpdates) throws SingleIndexWriteFailureException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Writing index update:" + mutations + " to table: " + tableReference);
    }
    Table table = null;
    try {
      if (allowLocalUpdates
              && env != null
              && tableReference.getTableName().equals(
                      env.getRegion().getTableDesc().getNameAsString())) {
        int failedCount = 0;
        while (true) {
          try {
            if (failedCount > 0) {
              TimeUnit.SECONDS.sleep(1);
            }
            IndexUtil.writeLocalUpdates(env.getRegion(), mutations, true);
            return;
          } catch (InterruptedException | RegionTooBusyException e) {
            LOG.info(e);
            ++failedCount;
          }
        }
      }
      table = factory.getTable(tableReference.get());
      table.batch(mutations);
    } catch (IOException e) {
      throw new SingleIndexWriteFailureException(tableReference.toString(), mutations, e);
    } catch (InterruptedException e) {
      // reset the interrupt status on the thread
      Thread.currentThread().interrupt();
      throw new SingleIndexWriteFailureException(tableReference.toString(), mutations, e);
    } finally {
      if (table != null) {
        try {
          table.close();
        } catch (IOException ex) {
          throw new SingleIndexWriteFailureException(tableReference.toString(), mutations, ex);
        }
      }
    }
  }

  @Override
  public void write(Multimap<HTableInterfaceReference, Mutation> toWrite, final boolean allowLocalUpdates) throws SingleIndexWriteFailureException {
    Set<Entry<HTableInterfaceReference, Collection<Mutation>>> entries = toWrite.asMap().entrySet();
    for (Entry<HTableInterfaceReference, Collection<Mutation>> entry : entries) {
      final HTableInterfaceReference tableReference = entry.getKey();
      if (env != null
              && !allowLocalUpdates
              && tableReference.getTableName().equals(
                      env.getRegion().getTableDesc().getNameAsString())) {
        continue;
      }
      // get the mutations for each table. We leak the implementation here a little bit to save
      // doing a complete copy over of all the index update for each table.
      final List<Mutation> mutations = kvBuilder.cloneIfNecessary((List<Mutation>) entry.getValue());
      updateIndexes(tableReference, mutations, allowLocalUpdates);
    }
  }

  /**
   * {@inheritDoc}
   * <p>
   * This method should only be called <b>once</b>. Stopped state
   * ({@link #isStopped()}) is managed by the external {@link Stoppable}. This
   * call does not delegate the stop down to the {@link Stoppable} passed in the
   * constructor.
   *
   * @param why the reason for stopping
   */
  @Override
  public void stop(String why) {
    LOG.info("Shutting down " + this.getClass().getSimpleName() + " because " + why);
    this.factory.shutdown();
  }

  @Override
  public boolean isStopped() {
    return this.stopped.isStopped();
  }
}
