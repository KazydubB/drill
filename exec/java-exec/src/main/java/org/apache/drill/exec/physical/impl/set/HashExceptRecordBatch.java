/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.physical.impl.set;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.logical.data.NamedExpression;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.memory.BaseAllocator;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.MetricDef;
import org.apache.drill.exec.physical.base.AbstractBase;
import org.apache.drill.exec.physical.config.Except;
import org.apache.drill.exec.physical.impl.aggregate.HashExceptProbe;
import org.apache.drill.exec.physical.impl.aggregate.HashExceptProbeTemplate;
import org.apache.drill.exec.physical.impl.aggregate.SpilledRecordbatch;
import org.apache.drill.exec.physical.impl.common.AbstractSpilledPartitionMetadata;
import org.apache.drill.exec.physical.impl.common.ChainedHashTable;
import org.apache.drill.exec.physical.impl.common.Comparator;
import org.apache.drill.exec.physical.impl.common.HashPartition;
import org.apache.drill.exec.physical.impl.common.HashTable;
import org.apache.drill.exec.physical.impl.common.HashTableConfig;
import org.apache.drill.exec.physical.impl.common.HashTableStats;
import org.apache.drill.exec.physical.impl.common.SpilledState;
import org.apache.drill.exec.physical.impl.join.HashJoinHelper;
import org.apache.drill.exec.physical.impl.join.HashJoinMechanicalMemoryCalculator;
import org.apache.drill.exec.physical.impl.join.HashJoinMemoryCalculator;
import org.apache.drill.exec.physical.impl.join.HashJoinMemoryCalculatorImpl;
import org.apache.drill.exec.physical.impl.spill.SpillSet;
import org.apache.drill.exec.planner.common.JoinControl;
import org.apache.drill.exec.record.AbstractBinaryRecordBatch;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.JoinBatchMemoryManager;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.VectorAccessibleUtilities;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.util.record.RecordBatchStats;
import org.apache.drill.exec.vector.IntVector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.complex.AbstractContainerVector;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class HashExceptRecordBatch extends AbstractBinaryRecordBatch<Except> {

  private static final Logger logger = LoggerFactory.getLogger(HashExceptRecordBatch.class);

  /**
   * The maximum number of records within each internal batch.
   */
  private final int RECORDS_PER_BATCH; // internal batches

  private boolean skipHashTableBuild; // when outer side is empty, and the join is inner or left (see DRILL-6755) // todo: change comment or remove it

  // Runtime generated class implementing HashExceptProbe interface
  private HashExceptProbe probe;

  private final List<NamedExpression> rightExpr; // todo: avoid mess with this right expr! // todo: Important!!

  /**
   * Names of the join columns. This names are used in order to help estimate
   * the size of the {@link HashTable}s.
   */
  @Deprecated // todo: rename/revisit
  private final Set<String> buildJoinColumns;

  // Fields used for partitioning
  /**
   * The number of {@link HashPartition}s. This is configured via a system
   * option and set in {@link #partitionNumTuning(int, HashJoinMemoryCalculator.BuildSidePartitioning)}.
   */
  private int numPartitions = 1; // must be 2 to the power of bitsInMask

  /**
   * The master class used to generate {@link HashTable}s.
   */
  private ChainedHashTable baseHashTable;
  private final MutableBoolean buildSideIsEmpty = new MutableBoolean(false);
  private final MutableBoolean probeSideIsEmpty = new MutableBoolean(false);
  private boolean canSpill = false; //true;
  private boolean wasKilled; // a kill was received, may need to clean spilled partns

  /**
   * This array holds the currently active {@link HashPartition}s.
   */
  private HashPartition[] partitions;

  // Number of records in the output container
  private int outputRecords;

  // Schema of the build side
  private BatchSchema buildSchema;
  // Schema of the probe side
  private BatchSchema probeSchema;

  // todo: this field is an index for column for storing hash value values?
  private int rightHVColPosition;
  private final BufferAllocator allocator;
  // Local fields for left/right incoming - may be replaced when reading from spilled
  private RecordBatch buildBatch;
  private RecordBatch probeBatch;

  /**
   * Flag indicating whether or not the first data holding build batch needs to be fetched.
   */
  private final MutableBoolean prefetchedBuild = new MutableBoolean(false);
  /**
   * Flag indicating whether or not the first data holding probe batch needs to be fetched.
   */
  private final MutableBoolean prefetchedProbe = new MutableBoolean(false);

  // For handling spilling
  private final SpillSet spillSet;

  protected Except popConfig;

  private final int originalPartition = -1; // the partition a secondary reads from
  // todo: rename to obey Java conventions
  private IntVector read_right_HV_vector; // HV vector that was read from the spilled batch
  private final int maxBatchesInMemory;

  /**
   * This holds information about the spilled partitions for the build and probe side.
   */
  public static class SpilledPartition extends AbstractSpilledPartitionMetadata { // todo: rename
    private final int innerSpilledBatches;
    private final String innerSpillFile;
    private int outerSpilledBatches;
    private String outerSpillFile;
    private boolean updatedOuter;

    public SpilledPartition(int cycle, int originPartition, int prevOriginPartition,
          int innerSpilledBatches, String innerSpillFile) {
      super(cycle, originPartition, prevOriginPartition);
      this.innerSpilledBatches = innerSpilledBatches;
      this.innerSpillFile = innerSpillFile;
    }

    public int getInnerSpilledBatches() {
      return innerSpilledBatches;
    }

    public String getInnerSpillFile() {
      return innerSpillFile;
    }

    public int getOuterSpilledBatches() {
      Preconditions.checkState(updatedOuter);
      return outerSpilledBatches;
    }

    public String getOuterSpillFile() {
      Preconditions.checkState(updatedOuter);
      return outerSpillFile;
    }

    public void updateOuter(int outerSpilledBatches, String outerSpillFile) {
      Preconditions.checkState(!updatedOuter);
      updatedOuter = true;

      this.outerSpilledBatches = outerSpilledBatches;
      this.outerSpillFile = outerSpillFile;
    }

    @Override
    public String makeDebugString() {
      return String.format("Start reading spilled partition %d (prev %d) from cycle %d (with %d-%d batches).",
          this.getOriginPartition(), this.getPrevOriginPartition(), this.getCycle(), outerSpilledBatches, innerSpilledBatches);
    }
  }

  public class HashJoinUpdater implements SpilledState.Updater { // todo: rename
    @Override
    public void cleanup() {
      HashExceptRecordBatch.this.cleanup();
    }

    @Override
    public String getFailureMessage() {
      return "Hash-Join can not partition the inner data any further (probably due to too many join-key duplicates).";
    }

    @Override
    public long getMemLimit() {
      return HashExceptRecordBatch.this.allocator.getLimit();
    }

    @Override
    public boolean hasPartitionLimit() {
      return true;
    }
  }

  /**
   * Queue of spilled partitions to process.
   */
  // todo: comment
  private final SpilledState<SpilledPartition> spilledState = new SpilledState<>();
  private final HashExceptRecordBatch.HashJoinUpdater spilledStateUpdater = new HashExceptRecordBatch.HashJoinUpdater();
  private SpilledPartition[] spilledInners; // for the outer to find the partition

  public enum Metric implements MetricDef {
    NUM_BUCKETS,
    NUM_ENTRIES,
    NUM_RESIZING,
    RESIZING_TIME_MS,
    NUM_PARTITIONS,
    SPILLED_PARTITIONS, // number of original partitions spilled to disk
    SPILL_MB,         // Number of MB of data spilled to disk. This amount is first written,
    // then later re-read. So, disk I/O is twice this amount.
    SPILL_CYCLE,       // 0 - no spill, 1 - spill, 2 - SECONDARY, 3 - TERTIARY
    LEFT_INPUT_BATCH_COUNT,
    LEFT_AVG_INPUT_BATCH_BYTES,
    LEFT_AVG_INPUT_ROW_BYTES,
    LEFT_INPUT_RECORD_COUNT,
    RIGHT_INPUT_BATCH_COUNT,
    RIGHT_AVG_INPUT_BATCH_BYTES,
    RIGHT_AVG_INPUT_ROW_BYTES,
    RIGHT_INPUT_RECORD_COUNT,
    OUTPUT_BATCH_COUNT,
    AVG_OUTPUT_BATCH_BYTES,
    AVG_OUTPUT_ROW_BYTES,
    OUTPUT_RECORD_COUNT;

    // duplicate for hash ag // todo:

    @Override
    public int metricId() { return ordinal(); }
  }

  @Override
  public int getRecordCount() {
    return outputRecords;
  }

  @Override
  protected void buildSchema() throws SchemaChangeException {
    // We must first get the schemas from upstream operators before we can build
    // our schema.
    boolean validSchema = prefetchFirstBatchFromBothSides();

    if (validSchema) {
      // We are able to construct a valid schema from the upstream data.
      // Setting the state here makes sure AbstractRecordBatch returns OK_NEW_SCHEMA
      state = BatchState.BUILD_SCHEMA;

      if (leftUpstream == IterOutcome.OK_NEW_SCHEMA) {
        probeSchema = left.getSchema(); // todo: do not reference left and right explicitly, but use build and probe batches instead?
      }

      if (rightUpstream == IterOutcome.OK_NEW_SCHEMA) {
        buildSchema = right.getSchema();
        // position of the new "column" for keeping the hash values
        // (after the real columns)
        rightHVColPosition = right.getContainer().getNumberOfColumns();
        // In special cases, when the probe side is empty, and
        // inner/left join - no need for Hash Table // todo: change the condition to our needs
        skipHashTableBuild = leftUpstream == IterOutcome.NONE; // && (control.isExcept() && !control.isDistinct()); // && ! joinIsRightOrFull;
        // We only need the hash tables if we have data on the build side.
        setupHashTable();
      }

      probe = setupHashJoinProbe();
    }

    // If we have a valid schema, this will build a valid container.
    // If we were unable to obtain a valid schema,
    // we still need to build a dummy schema. This code handles both cases for us.
    setupOutputContainerSchema();
    container.buildSchema(BatchSchema.SelectionVectorMode.NONE);
    container.setEmpty();
  }

  /**
   * Prefetches the first build side data holding batch.
   */
  private void prefetchFirstBuildBatch() {
    rightUpstream = prefetchFirstBatch(rightUpstream,
        prefetchedBuild,
        buildSideIsEmpty,
        RIGHT_INDEX,
        buildBatch,
        () -> {
          batchMemoryManager.update(RIGHT_INDEX, 0, true);
          RecordBatchStats.logRecordBatchStats(RecordBatchStats.RecordBatchIOType.INPUT_RIGHT,
              batchMemoryManager.getRecordBatchSizer(RIGHT_INDEX),
              getRecordBatchStatsContext());
        });
  }

  /**
   * Prefetches the first build side data holding batch.
   */
  private void prefetchFirstProbeBatch() {
    leftUpstream =  prefetchFirstBatch(leftUpstream,
        prefetchedProbe,
        probeSideIsEmpty,
        LEFT_INDEX,
        probeBatch,
        () -> {
          batchMemoryManager.update(LEFT_INDEX, 0);
          RecordBatchStats.logRecordBatchStats(RecordBatchStats.RecordBatchIOType.INPUT_LEFT,
              batchMemoryManager.getRecordBatchSizer(LEFT_INDEX),
              getRecordBatchStatsContext());
        });
  }

  /**
   * Used to fetch the first data holding batch from either the build or probe side.
   * @param outcome The current upstream outcome for either the build or probe side.
   * @param prefetched A flag indicating if we have already done a prefetch of the first data holding batch for the probe or build side.
   * @param isEmpty A flag indicating if the probe or build side is empty.
   * @param index The upstream index of the probe or build batch.
   * @param batch The probe or build batch itself.
   * @param memoryManagerUpdate A lambda function to execute the memory manager update for the probe or build batch.
   * @return The current {@link org.apache.drill.exec.record.RecordBatch.IterOutcome}.
   */
  private IterOutcome prefetchFirstBatch(IterOutcome outcome,
                                         MutableBoolean prefetched,
                                         MutableBoolean isEmpty,
                                         int index,
                                         RecordBatch batch,
                                         Runnable memoryManagerUpdate) {
    if (prefetched.booleanValue()) {
      // We have already prefetch the first data holding batch
      return outcome;
    }

    // If we didn't retrieve our first data holding batch, we need to do it now.
    prefetched.setValue(true);

    if (outcome != IterOutcome.NONE) {
      // We can only get data if there is data available
      outcome = sniffNonEmptyBatch(outcome, index, batch);
    }

    isEmpty.setValue(outcome == IterOutcome.NONE); // If we received NONE there is no data.

    if (outcome == IterOutcome.STOP) {
      // We reached a termination state
      state = BatchState.STOP;
    } else {
      // Got our first batch(es)
      if (spilledState.isFirstCycle()) {
        // Only collect stats for the first cycle
        memoryManagerUpdate.run();
      }
      state = BatchState.FIRST;
    }

    return outcome;
  }

  /**
   * Currently in order to accurately predict memory usage for spilling, the first non-empty build or probe side batch is needed. This method
   * fetches the first non-empty batch from the probe or build side.
   * @param curr The current outcome.
   * @param inputIndex Index specifying whether to work with the prorbe or build input.
   * @param recordBatch The probe or build record batch.
   * @return The {@link org.apache.drill.exec.record.RecordBatch.IterOutcome} for the left or right record batch.
   */
//  @Deprecated // todo: remove deprecation and use it?
  private IterOutcome sniffNonEmptyBatch(IterOutcome curr, int inputIndex, RecordBatch recordBatch) {
    while (true) {
      if (recordBatch.getRecordCount() != 0) {
        return curr;
      }

      curr = next(inputIndex, recordBatch);

      switch (curr) {
        case OK:
          // We got a data batch
          break;
        case NOT_YET:
          // We need to try again
          break;
        case EMIT:
          throw new UnsupportedOperationException("We do not support " + IterOutcome.EMIT);
        default:
          // Other cases are termination conditions
          return curr;
      }
    }
  }

  /**
   * Determines the memory calculator to use. If maxNumBatches is configured simple batch counting is used to spill. Otherwise
   * memory calculations are used to determine when to spill.
   * @return The memory calculator to use.
   */
  // todo: change to another calculator class
  public HashJoinMemoryCalculator getCalculatorImpl() { // todo: use another class
    if (maxBatchesInMemory == 0) {
      double safetyFactor = context.getOptions().getDouble(ExecConstants.HASHJOIN_SAFETY_FACTOR_KEY);
      double fragmentationFactor = context.getOptions().getDouble(ExecConstants.HASHJOIN_FRAGMENTATION_FACTOR_KEY);
      double hashTableDoublingFactor = context.getOptions().getDouble(ExecConstants.HASHJOIN_HASH_DOUBLE_FACTOR_KEY);
      String hashTableCalculatorType = context.getOptions().getString(ExecConstants.HASHJOIN_HASHTABLE_CALC_TYPE_KEY);

//      return new HashJoinMemoryCalculatorImpl(safetyFactor, fragmentationFactor, hashTableDoublingFactor, hashTableCalculatorType, semiJoin);
      return new HashJoinMemoryCalculatorImpl(safetyFactor, fragmentationFactor, hashTableDoublingFactor, hashTableCalculatorType, false);
    } else {
      return new HashJoinMechanicalMemoryCalculator(maxBatchesInMemory);
    }
  }

  @Override
  public IterOutcome innerNext() {
    if (wasKilled) {
      // We have received a kill signal. We need to stop processing.
      this.cleanup();
      super.close();
      return IterOutcome.NONE;
    }

    prefetchFirstBuildBatch();

    if (rightUpstream.isError()) {
      // A termination condition was reached while prefetching the first build side data holding batch.
      // We need to terminate.
      return rightUpstream;
    }

    try {
      /* If we are here for the first time, execute the build phase of the
       * hash join and setup the run time generated class for the probe side
       */
      if (state == BatchState.FIRST) {
        // Build the hash table, using the build side record batches.
        IterOutcome buildExecuteTermination = executeBuildPhase(); // todo: important

        if (buildExecuteTermination != null) {
          // A termination condition was reached while executing the build phase.
          // We need to terminate.
          return buildExecuteTermination;
        }

        // Update the hash table related stats for the operator
        updateStats();
      }

      // Try to probe and project, or recursively handle a spilled partition
      if (!buildSideIsEmpty.booleanValue() /*||  // If there are build-side rows
          joinIsLeftOrFull*/) {  // or if this is a left/full outer join // todo: change

        prefetchFirstProbeBatch();

        if (leftUpstream.isError() ||
            (leftUpstream == IterOutcome.NONE /*&& ! joinIsRightOrFull */)) { // todo: change to the needs
          // A termination condition was reached while prefetching the first probe side data holding batch.
          // We need to terminate.
          return leftUpstream;
        }

        if (!buildSideIsEmpty.booleanValue() || !probeSideIsEmpty.booleanValue()) {
          // Only allocate outgoing vectors and execute probing logic if there is data

          if (state == BatchState.FIRST) {
            // Initialize various settings for the probe side
            probe.setupHashSetProbe(probeBatch, this, popConfig.isAll(), leftUpstream, partitions,
                spilledState.getCycle(), container, spilledInners, buildSideIsEmpty.booleanValue(),
                numPartitions, rightHVColPosition);
          }

          // Allocate the memory for the vectors in the output container
          batchMemoryManager.allocateVectors(container);

          // hashJoinProbe.setTargetOutputCount(batchMemoryManager.getOutputRowCount());
          probe.setTargetOutputCount(batchMemoryManager.getOutputRowCount());

          outputRecords = probe.probeAndProject();

          container.setValueCount(outputRecords);

          batchMemoryManager.updateOutgoingStats(outputRecords);
          RecordBatchStats.logRecordBatchStats(RecordBatchStats.RecordBatchIOType.OUTPUT, this, getRecordBatchStatsContext());

          /* We are here because of one the following
           * 1. Completed processing of all the records and we are done
           * 2. We've filled up the outgoing batch to the maximum and we need to return upstream
           * Either case build the output container's schema and return
           */
          if (outputRecords > 0 || state == BatchState.FIRST) {
            state = BatchState.NOT_FIRST;

            return IterOutcome.OK;
          }
        }

        // Free all partitions' in-memory data structures
        // (In case need to start processing spilled partitions)
        for (HashPartition partition : partitions) {
          partition.cleanup(false); // clean, but do not delete the spill files
        }

        //  (recursively) handle the spilled partitions, if any
        if (!buildSideIsEmpty.booleanValue()) {
          while (!spilledState.isEmpty()) {  // "while" is only used for skipping; see "continue" below

            // Get the next (previously) spilled partition to handle as incoming
            SpilledPartition spilledPartition = spilledState.getNextSpilledPartition();

            // If the outer is empty (and it's not a right/full join) - try the next spilled partition
            if (spilledPartition.outerSpilledBatches == 0 /*&& !joinIsRightOrFull*/) { // todo: change
              continue;
            }

            // Create a BUILD-side "incoming" out of the inner spill file of that partition
            buildBatch = new SpilledRecordbatch(spilledPartition.innerSpillFile, spilledPartition.innerSpilledBatches, context, buildSchema, oContext, spillSet);
            // The above ctor call also got the first batch; need to update the outcome
            rightUpstream = ((SpilledRecordbatch) buildBatch).getInitialOutcome();

            if (spilledPartition.outerSpilledBatches > 0) {
              // Create a PROBE-side "incoming" out of the outer spill file of that partition
              probeBatch = new SpilledRecordbatch(spilledPartition.outerSpillFile, spilledPartition.outerSpilledBatches, context, probeSchema, oContext, spillSet);
              // The above ctor call also got the first batch; need to update the outcome
              leftUpstream = ((SpilledRecordbatch) probeBatch).getInitialOutcome();
            } else {
              probeBatch = left; // if no outer batch then reuse left - needed for updateIncoming()
              leftUpstream = IterOutcome.NONE;
              probe.markAsDone();
            }

            spilledState.updateCycle(stats, spilledPartition, spilledStateUpdater);
            state = BatchState.FIRST;  // TODO need to determine if this is still necessary since prefetchFirstBatchFromBothSides sets this

            prefetchedBuild.setValue(false);
            prefetchedProbe.setValue(false);

            return innerNext(); // start processing the next spilled partition "recursively"
          }
        }

      } else {
        // Our build side is empty, we won't have any matches, clear the probe side
        killAndDrainLeftUpstream();
      }

      // No more output records, clean up and return
      state = BatchState.DONE;

      cleanup();

      return IterOutcome.NONE;
    } catch (SchemaChangeException e) {
      context.getExecutorState().fail(e);
      killIncoming(false);
      return IterOutcome.STOP;
    }
  }

  /**
   *  In case an upstream data is no longer needed, send a kill and flush any remaining batch
   *
   * @param batch probe or build batch
   * @param upstream which upstream
   * @param isLeft is it the left or right
   */
  private void killAndDrainUpstream(RecordBatch batch, IterOutcome upstream, boolean isLeft) {
    batch.kill(true);
    while (upstream == IterOutcome.OK_NEW_SCHEMA || upstream == IterOutcome.OK) {
      VectorAccessibleUtilities.clear(batch);
      upstream = next(isLeft ? HashJoinHelper.LEFT_INPUT : HashJoinHelper.RIGHT_INPUT, batch);
    }
  }

  private void killAndDrainLeftUpstream() {
    killAndDrainUpstream(probeBatch, leftUpstream, true);
  }

  private void killAndDrainRightUpstream() {
    killAndDrainUpstream(buildBatch, rightUpstream, false);
  }

  private void setupHashTable() throws SchemaChangeException {
    if (skipHashTableBuild) {
      return;
    }

    List<Comparator> comparators = new ArrayList<>(popConfig.getLeftFields().size());
    popConfig.getLeftFields().forEach(f -> comparators.add(Comparator.IS_NOT_DISTINCT_FROM));

    List<NamedExpression> leftExpr = new ArrayList<>(popConfig.getLeftExpressions());

    // Set the left named expression to be null if the probe batch is empty.
    if (leftUpstream != IterOutcome.OK_NEW_SCHEMA && leftUpstream != IterOutcome.OK) {
      leftExpr = null;
    } else {
      if (probeBatch.getSchema().getSelectionVectorMode() != BatchSchema.SelectionVectorMode.NONE) {
        String errorMsg = "Hash Set Operator does not support probe batch with selection vectors. " +
            "Probe batch has selection mode = " + probeBatch.getSchema().getSelectionVectorMode();
        throw new SchemaChangeException(errorMsg);
      }
    }

    boolean keepCount = popConfig.isAll();
    // todo: avoid passing JoinControl.DEFAULT :) (by introducing new constructor w/o the field
    HashTableConfig htConfig = new HashTableConfig((int) context.getOptions().getOption(ExecConstants.MIN_HASH_TABLE_SIZE),
        true, HashTable.DEFAULT_LOAD_FACTOR, rightExpr, leftExpr, comparators, JoinControl.DEFAULT, keepCount); // todo: rightExpr and leftExpr...

    // Create the chained hash table
    baseHashTable = new ChainedHashTable(htConfig, context, allocator, buildBatch, probeBatch, null); /// todo: outgoing is always null! Should be `this`?
  }

  /**
   *  Call only after number of partitions is known
   */
  private void delayedSetup() {
    //  Find out the estimated max batch size, etc
    //  and compute the max numPartitions possible
    //  See partitionNumTuning()
    spilledState.initialize(numPartitions);
    // Create array for the partitions
    partitions = new HashPartition[numPartitions];
  }

  /**
   * Initialize fields (that may be reused when reading spilled partitions)
   */
  private void initializeBuild() {
    baseHashTable.updateIncoming(buildBatch, probeBatch); // in case we process the spilled files
    // Recreate the partitions every time build is initialized
    for (int part = 0; part < numPartitions; part++) {
      partitions[part] = new HashPartition(context, allocator, baseHashTable, buildBatch, probeBatch, false/*semiJoin*/, // todo; semiJoin = true!
          RECORDS_PER_BATCH, spillSet, part, spilledState.getCycle(), numPartitions);
    }

    spilledInners = new SpilledPartition[numPartitions];
  }

  /**
   * Tunes the number of partitions used by {@link org.apache.drill.exec.physical.impl.join.HashJoinBatch}. If it is not possible to spill it gives up and reverts
   * to unbounded in memory operation.
   * @param maxBatchSize
   * @param buildCalc
   * @return
   */
  private HashJoinMemoryCalculator.BuildSidePartitioning partitionNumTuning(
      int maxBatchSize,
      HashJoinMemoryCalculator.BuildSidePartitioning buildCalc) {
    // Get auto tuning result
    numPartitions = buildCalc.getNumPartitions();

    if (logger.isDebugEnabled()) {
      logger.debug(buildCalc.makeDebugString());
    }

    if (buildCalc.getMaxReservedMemory() > allocator.getLimit()) {
      // We don't have enough memory to do any spilling. Give up and do no spilling and have no limits

      // TODO dirty hack to prevent regressions. Remove this once batch sizing is implemented.
      // We don't have enough memory to do partitioning, we have to do everything in memory
      String message = String.format("When using the minimum number of partitions %d we require %s memory but only have %s available. " +
              "Forcing legacy behavoir of using unbounded memory in order to prevent regressions.",
          numPartitions,
          FileUtils.byteCountToDisplaySize(buildCalc.getMaxReservedMemory()),
          FileUtils.byteCountToDisplaySize(allocator.getLimit()));
      logger.warn(message);

      // create a Noop memory calculator
      HashJoinMemoryCalculator calc = getCalculatorImpl();
      calc.initialize(false);
      buildCalc = calc.next();

      buildCalc.initialize(true, true, // TODO Fix after growing hash values bug fixed
          buildBatch, probeBatch, buildJoinColumns, leftUpstream == IterOutcome.NONE, // probeEmpty
          allocator.getLimit(), numPartitions, RECORDS_PER_BATCH, RECORDS_PER_BATCH,
          maxBatchSize, maxBatchSize, batchMemoryManager.getOutputBatchSize(), HashTable.DEFAULT_LOAD_FACTOR);
      disableSpilling(null);
    }

    return buildCalc;
  }

  /**
   * Disable spilling - use only a single partition and set the memory limit to the max (10GB)
   * @param reason If not null - log this as warning, else check fallback setting to either warn or fail.
   */
  private void disableSpilling(String reason) {
    // Fail, or just issue a warning if a reason was given, or a fallback option is enabled
    if (reason == null) {
      boolean fallbackEnabled = context.getOptions().getOption(ExecConstants.HASHJOIN_FALLBACK_ENABLED_KEY).bool_val;
      if (fallbackEnabled) {
        logger.warn("Spilling is disabled - not enough memory available for internal partitioning. Falling back" +
            " to use unbounded memory");
      } else {
        throw UserException.resourceError().message(String.format("Not enough memory for internal partitioning and fallback mechanism for " +
            "HashJoin to use unbounded memory is disabled. Either enable fallback config %s using Alter " +
            "session/system command or increase memory limit for Drillbit", ExecConstants.HASHJOIN_FALLBACK_ENABLED_KEY)).build(logger);
      }
    } else {
      logger.warn(reason);
    }

    numPartitions = 1; // We are only using one partition
    canSpill = false; // We cannot spill
    allocator.setLimit(AbstractBase.MAX_ALLOCATION); // Violate framework and force unbounded memory
  }

  /**
   *  Execute the BUILD phase; first read incoming and split rows into partitions;
   *  may decide to spill some of the partitions
   *
   * @return Returns an {@link org.apache.drill.exec.record.RecordBatch.IterOutcome} if a termination condition is reached. Otherwise returns null.
   * @throws SchemaChangeException
   */
  public IterOutcome executeBuildPhase() throws SchemaChangeException {
    if (buildSideIsEmpty.booleanValue()) {
      // empty right // todo: comment...
      return null;
    }

    if (skipHashTableBuild) { // No hash table needed - then consume all the right upstream
      killAndDrainRightUpstream();
      return null;
    }
    // todo: do not use HashJoin memory calculator!
    HashJoinMemoryCalculator.BuildSidePartitioning buildCalc;

    // Initializing build calculator
    // Limit scope of these variables to this block
    int maxBatchSize = spilledState.isFirstCycle()? RecordBatch.MAX_BATCH_ROW_COUNT: RECORDS_PER_BATCH;
    boolean doMemoryCalculation = canSpill && !probeSideIsEmpty.booleanValue();
    HashJoinMemoryCalculator calc = getCalculatorImpl();

    calc.initialize(doMemoryCalculation);
    buildCalc = calc.next();

    buildCalc.initialize(spilledState.isFirstCycle(), true, // TODO Fix after growing hash values bug fixed
        buildBatch, probeBatch, buildJoinColumns, probeSideIsEmpty.booleanValue(),
        allocator.getLimit(), numPartitions, RECORDS_PER_BATCH, RECORDS_PER_BATCH,
        maxBatchSize, maxBatchSize, batchMemoryManager.getOutputBatchSize(), HashTable.DEFAULT_LOAD_FACTOR);

    if (spilledState.isFirstCycle() && doMemoryCalculation) {
      // Do auto tuning
      buildCalc = partitionNumTuning(maxBatchSize, buildCalc);
    }

    if (spilledState.isFirstCycle()) {
      // Do initial setup only on the first cycle
      delayedSetup();
    }

    initializeBuild();

    // Make the calculator aware of our partitions
    HashJoinMemoryCalculator.PartitionStatSet partitionStatSet = new HashJoinMemoryCalculator.PartitionStatSet(partitions);
    buildCalc.setPartitionStatSet(partitionStatSet);

    boolean moreData = true;
    while (moreData) {
      switch (rightUpstream) {
        case NONE:
        case NOT_YET:
        case STOP:
          moreData = false;
          continue;

        case OK_NEW_SCHEMA:
          if (!buildSchema.equals(buildBatch.getSchema())) {
            throw SchemaChangeException.schemaChanged("Hash join does not support schema changes in build side.", buildSchema, buildBatch.getSchema());
          }
          for (HashPartition partn : partitions) { partn.updateBatches(); }
          // Fall through
        case OK:
          batchMemoryManager.update(buildBatch, RIGHT_INDEX, 0, true);
          int currentRecordCount = buildBatch.getRecordCount();
          // Special treatment (when no spill, and single partition) -- use the incoming vectors as they are (no row copy)
          if (numPartitions == 1) {
            partitions[0].appendBatch(buildBatch);
            break;
          }

          if (!spilledState.isFirstCycle()) {
            // todo: must ensure this is actually the HashValue column!
            read_right_HV_vector = (IntVector) buildBatch.getContainer().getLast();
          }

          // For every record in the build batch, hash the key columns and keep the result
          for (int ind = 0; ind < currentRecordCount; ind++) {
            int hashCode = spilledState.isFirstCycle() ? partitions[0].getBuildHashCode(ind)
                : read_right_HV_vector.getAccessor().get(ind); // get the hash value from the HV column
            int currPart = hashCode & spilledState.getPartitionMask();
            hashCode >>>= spilledState.getBitsInMask();
            // semi-join skips join-key-duplicate rows // todo: ??
            /*if (semiJoin) { // todo: understand what this means, lol

            }*/
            // Append the new inner row to the appropriate partition; spill (that partition) if needed
            partitions[currPart].appendInnerRow(buildBatch.getContainer(), ind, hashCode, buildCalc);
          }

          if (read_right_HV_vector != null) {
            read_right_HV_vector.clear();
            read_right_HV_vector = null;
          }
          break;
        default:
          throw new IllegalStateException(rightUpstream.name());
      }
      // Get the next incoming record batch
      rightUpstream = next(HashJoinHelper.RIGHT_INPUT, buildBatch);
    }

    // Move the remaining current batches into their temp lists, or spill
    // them if the partition is spilled. Add the spilled partitions into
    // the spilled partitions list
    if (numPartitions > 1) { // a single partition needs no completion
      for (HashPartition partn : partitions) {
        partn.completeAnInnerBatch(false, partn.isSpilled());
      }
    }

    prefetchFirstProbeBatch();

    if (leftUpstream.isError()) {
      // A termination condition was reached while prefetching the first build side data holding batch.
      // We need to terminate.
      return leftUpstream;
    }

    HashJoinMemoryCalculator.PostBuildCalculations postBuildCalc = buildCalc.next();
    postBuildCalc.initialize(probeSideIsEmpty.booleanValue()); // probeEmpty

    //  Traverse all the in-memory partitions' incoming batches, and build their hash tables

    for (int index = 0; index < partitions.length; index++) {
      HashPartition partn = partitions[index];

      if (partn.isSpilled()) {
        // Don't build hash tables for spilled partitions
        continue;
      }

      try {
        if (postBuildCalc.shouldSpill()) {
          // Spill this partition if we need to make room
          partn.spillThisPartition();
        } else {
          // Only build hash tables for partitions that are not spilled
          partn.buildContainersHashTableAndHelper();
        }
      } catch (OutOfMemoryException e) {
        String message = "Failed building hash table on partition " + index + ":\n"
            + makeDebugString() + "\n"
            + postBuildCalc.makeDebugString();
        // Include debug info
        throw new OutOfMemoryException(message, e);
      }
    }

    if (logger.isDebugEnabled()) {
      logger.debug(postBuildCalc.makeDebugString());
    }

    for (HashPartition partition : partitions) {
      if (partition.isSpilled()) {
        SpilledPartition sp = new SpilledPartition(spilledState.getCycle(),
            partition.getPartitionNum(),
            originalPartition,
            partition.getPartitionBatchesCount(),
            partition.getSpillFile());

        spilledState.addPartition(sp);
        spilledInners[partition.getPartitionNum()] = sp; // for the outer to find the SP later
        partition.closeWriter();

        partition.updateProbeRecordsPerBatch(postBuildCalc.getProbeRecordsPerBatch());
      }
    }

    return null;
  }

  private void setupOutputContainerSchema() {
    if (probeSchema != null) { // a probe schema was seen (even though the probe may had no rows)
      for (VectorWrapper<?> vv : probeBatch) {
        TypeProtos.MajorType inputType = vv.getField().getType();
        TypeProtos.MajorType outputType;

        // If right or full outer join then the output type should be optional. However, map types are
        // not nullable so we must exclude them from the check below (see DRILL-2771, DRILL-2197).
//        if (joinIsRightOrFull && inputType.getMode() == TypeProtos.DataMode.REQUIRED
//        if (operatorControl.isExcept() && inputType.getMode() == TypeProtos.DataMode.REQUIRED
//        if (false && inputType.getMode() == TypeProtos.DataMode.REQUIRED // todo: is this OK to treat it as left join (thus isRightOrFull is == false)?
//            && inputType.getMinorType() != TypeProtos.MinorType.MAP) {
//          outputType = Types.overrideMode(inputType, TypeProtos.DataMode.OPTIONAL);
//        } else {
        outputType = inputType; // todo: only this line was before
//        }

        ValueVector v = container.addOrGet(MaterializedField.create(vv.getField().getName(), outputType));
        if (v instanceof AbstractContainerVector) {
          vv.getValueVector().makeTransferPair(v);
          v.clear();
        }
      }
    }
  }

  // (After the inner side was read whole) - Has that inner partition spilled
  public boolean isSpilledInner(int part) {
    if (spilledInners == null) { // empty inner
      return false;
    }
    return spilledInners[part] != null;
  }

  /**
   *  The constructor
   *
   * @param popConfig
   * @param context
   * @param left  -- probe/outer side incoming input
   * @param right -- build/iner side incoming input
   * @throws OutOfMemoryException
   */
  public HashExceptRecordBatch(Except popConfig, FragmentContext context,
        RecordBatch left, RecordBatch right) throws OutOfMemoryException {
    super(popConfig, context, true, left, right);

    if (popConfig.isDistinct()) {
      this.buildBatch = left;
      this.probeBatch = right;
    } else {
      this.buildBatch = right;
      this.probeBatch = left;
    }

    rightExpr = new ArrayList<>(popConfig.getRightExpressions());
    this.popConfig = popConfig;
    buildJoinColumns = new HashSet<>();
    allocator = oContext.getAllocator();

    numPartitions = 1;// (int) context.getOptions().getOption(ExecConstants.HASHJOIN_NUM_PARTITIONS_VALIDATOR); // todo: support spilling!?
    if (numPartitions == 1) { //
      disableSpilling("Spilling is disabled due to configuration setting of num_partitions to 1");
    }

    numPartitions = BaseAllocator.nextPowerOfTwo(numPartitions); // in case not a power of 2

    long memLimit = context.getOptions().getOption(ExecConstants.HASHJOIN_MAX_MEMORY_VALIDATOR);

    if (memLimit != 0) {
      allocator.setLimit(memLimit);
    }

    RECORDS_PER_BATCH = (int) context.getOptions().getOption(ExecConstants.HASHJOIN_NUM_ROWS_IN_BATCH_VALIDATOR);
    maxBatchesInMemory = (int) context.getOptions().getOption(ExecConstants.HASHJOIN_MAX_BATCHES_IN_MEMORY_VALIDATOR);

    logger.info("Memory limit {} bytes", FileUtils.byteCountToDisplaySize(allocator.getLimit()));
    spillSet = new SpillSet(context, popConfig);

    // Create empty partitions (in the ctor - covers the case where right side is empty)
    partitions = new HashPartition[0];

    // get the output batch size from config.
    int configuredBatchSize = (int) context.getOptions().getOption(ExecConstants.OUTPUT_BATCH_SIZE_VALIDATOR);
    double avail_mem_factor = context.getOptions().getOption(ExecConstants.OUTPUT_BATCH_SIZE_AVAIL_MEM_FACTOR_VALIDATOR);
    int outputBatchSize = Math.min(configuredBatchSize, Integer.highestOneBit((int)(allocator.getLimit() * avail_mem_factor)));

    RecordBatchStats.logRecordBatchStats(getRecordBatchStatsContext(),
        "configured output batch size: %d, allocated memory %d, avail mem factor %f, output batch size: %d",
        configuredBatchSize, allocator.getLimit(), avail_mem_factor, outputBatchSize);

    batchMemoryManager = new JoinBatchMemoryManager(outputBatchSize, left, right, new HashSet<>()); // todo: introduce new class or rename the existing one?

    RecordBatchStats.printConfiguredBatchSize(getRecordBatchStatsContext(),
        configuredBatchSize);
  }

  /**
   * This method is called when {@link HashExceptRecordBatch} closes. It cleans up left over spilled files that are in the spill queue, and closes the
   * spillSet.
   */
  private void cleanup() {
    if (buildSideIsEmpty.booleanValue()) { // not set up; nothing to clean
      return;
    }
    if (spillSet.getWriteBytes() > 0) {
      stats.setLongStat(Metric.SPILL_MB, // update stats - total MB spilled
          (int) Math.round(spillSet.getWriteBytes() / 1024.0D / 1024.0));
    }
    // clean (and deallocate) each partition, and delete its spill file
    for (HashPartition partn : partitions) {
      partn.close();
    }

    // delete any spill file left in unread spilled partitions
    while (!spilledState.isEmpty()) {
      SpilledPartition sp = spilledState.getNextSpilledPartition();
      try {
        spillSet.delete(sp.innerSpillFile);
      } catch(IOException e) {
        logger.warn("Cleanup: Failed to delete spill file {}",sp.innerSpillFile);
      }
      if (sp.outerSpillFile != null) {
        try { // outer file is added later; may be null if cleaning prematurely
          spillSet.delete(sp.outerSpillFile);
        } catch (IOException e) {
          logger.warn("Cleanup: Failed to delete spill file {}", sp.outerSpillFile);
        }
      }
    }
    // Delete the currently handled (if any) spilled files
    spillSet.close(); // delete the spill directory(ies)
  }

  /**
   * This creates a string that summarizes the memory usage of the operator.
   * @return A memory dump string.
   */
  // todo: remove?
  public String makeDebugString() {
    StringBuilder sb = new StringBuilder();

    for (int partitionIndex = 0; partitionIndex < partitions.length; partitionIndex++) {
      String partitionPrefix = "Partition " + partitionIndex + ": ";
      HashPartition hashPartition = partitions[partitionIndex];
      sb.append(partitionPrefix).append(hashPartition.makeDebugString()).append("\n");
    }

    return sb.toString();
  }

  /**
   * Updates the {@link HashTable} and spilling stats after the original build side is processed.
   *
   * Note: this does not update all the stats. The cycleNum is updated dynamically in {@link #innerNext()} and the total bytes
   * written is updated at close time in {@link #cleanup()}.
   */
  private void updateStats() {
    if (buildSideIsEmpty.booleanValue()) { // no stats when the right side is empty
      return;
    }
    if (!spilledState.isFirstCycle()) { // These stats are only for before processing spilled files
      return;
    }

    HashTableStats htStats = new HashTableStats();
    long numSpilled = 0;
    HashTableStats newStats = new HashTableStats();
    // sum the stats from all the partitions
    for (HashPartition partn : partitions) {
      if (partn.isSpilled()) {
        numSpilled++;
      }
      partn.getStats(newStats);
      htStats.addStats(newStats);
    }

    this.stats.setLongStat(org.apache.drill.exec.physical.impl.join.HashJoinBatch.Metric.NUM_BUCKETS, htStats.numBuckets);
    this.stats.setLongStat(org.apache.drill.exec.physical.impl.join.HashJoinBatch.Metric.NUM_ENTRIES, htStats.numEntries);
    this.stats.setLongStat(org.apache.drill.exec.physical.impl.join.HashJoinBatch.Metric.NUM_RESIZING, htStats.numResizing);
    this.stats.setLongStat(org.apache.drill.exec.physical.impl.join.HashJoinBatch.Metric.RESIZING_TIME_MS, htStats.resizingTime);
    this.stats.setLongStat(org.apache.drill.exec.physical.impl.join.HashJoinBatch.Metric.NUM_PARTITIONS, numPartitions);
    this.stats.setLongStat(org.apache.drill.exec.physical.impl.join.HashJoinBatch.Metric.SPILL_CYCLE, spilledState.getCycle()); // Put 0 in case no spill
    this.stats.setLongStat(org.apache.drill.exec.physical.impl.join.HashJoinBatch.Metric.SPILLED_PARTITIONS, numSpilled);
  }

  @Override
  public void killIncoming(boolean sendUpstream) {
    wasKilled = true;
    probeBatch.kill(sendUpstream);
    buildBatch.kill(sendUpstream);
  }

  public void updateMetrics() {
    stats.setLongStat(org.apache.drill.exec.physical.impl.join.HashJoinBatch.Metric.LEFT_INPUT_BATCH_COUNT, batchMemoryManager.getNumIncomingBatches(LEFT_INDEX));
    stats.setLongStat(org.apache.drill.exec.physical.impl.join.HashJoinBatch.Metric.LEFT_AVG_INPUT_BATCH_BYTES, batchMemoryManager.getAvgInputBatchSize(LEFT_INDEX));
    stats.setLongStat(org.apache.drill.exec.physical.impl.join.HashJoinBatch.Metric.LEFT_AVG_INPUT_ROW_BYTES, batchMemoryManager.getAvgInputRowWidth(LEFT_INDEX));
    stats.setLongStat(org.apache.drill.exec.physical.impl.join.HashJoinBatch.Metric.LEFT_INPUT_RECORD_COUNT, batchMemoryManager.getTotalInputRecords(LEFT_INDEX));

    stats.setLongStat(org.apache.drill.exec.physical.impl.join.HashJoinBatch.Metric.RIGHT_INPUT_BATCH_COUNT, batchMemoryManager.getNumIncomingBatches(RIGHT_INDEX));
    stats.setLongStat(org.apache.drill.exec.physical.impl.join.HashJoinBatch.Metric.RIGHT_AVG_INPUT_BATCH_BYTES, batchMemoryManager.getAvgInputBatchSize(RIGHT_INDEX));
    stats.setLongStat(org.apache.drill.exec.physical.impl.join.HashJoinBatch.Metric.RIGHT_AVG_INPUT_ROW_BYTES, batchMemoryManager.getAvgInputRowWidth(RIGHT_INDEX));
    stats.setLongStat(org.apache.drill.exec.physical.impl.join.HashJoinBatch.Metric.RIGHT_INPUT_RECORD_COUNT, batchMemoryManager.getTotalInputRecords(RIGHT_INDEX));

    stats.setLongStat(org.apache.drill.exec.physical.impl.join.HashJoinBatch.Metric.OUTPUT_BATCH_COUNT, batchMemoryManager.getNumOutgoingBatches());
    stats.setLongStat(org.apache.drill.exec.physical.impl.join.HashJoinBatch.Metric.AVG_OUTPUT_BATCH_BYTES, batchMemoryManager.getAvgOutputBatchSize());
    stats.setLongStat(org.apache.drill.exec.physical.impl.join.HashJoinBatch.Metric.AVG_OUTPUT_ROW_BYTES, batchMemoryManager.getAvgOutputRowWidth());
    stats.setLongStat(org.apache.drill.exec.physical.impl.join.HashJoinBatch.Metric.OUTPUT_RECORD_COUNT, batchMemoryManager.getTotalOutputRecords());
  }

  @Override
  public void close() {
    if (!spilledState.isFirstCycle()) { // spilling happened
      // In case closing due to cancellation, BaseRootExec.close() does not close the open
      // SpilledRecordBatch "scanners" as it only knows about the original left/right ops.
      killIncoming(false);
    }

    updateMetrics();

    RecordBatchStats.logRecordBatchStats(getRecordBatchStatsContext(),
        "incoming aggregate left: batch count : %d, avg bytes : %d,  avg row bytes : %d, record count : %d",
        batchMemoryManager.getNumIncomingBatches(JoinBatchMemoryManager.LEFT_INDEX), batchMemoryManager.getAvgInputBatchSize(JoinBatchMemoryManager.LEFT_INDEX),
        batchMemoryManager.getAvgInputRowWidth(JoinBatchMemoryManager.LEFT_INDEX), batchMemoryManager.getTotalInputRecords(JoinBatchMemoryManager.LEFT_INDEX));

    RecordBatchStats.logRecordBatchStats(getRecordBatchStatsContext(),
        "incoming aggregate right: batch count : %d, avg bytes : %d,  avg row bytes : %d, record count : %d",
        batchMemoryManager.getNumIncomingBatches(JoinBatchMemoryManager.RIGHT_INDEX), batchMemoryManager.getAvgInputBatchSize(JoinBatchMemoryManager.RIGHT_INDEX),
        batchMemoryManager.getAvgInputRowWidth(JoinBatchMemoryManager.RIGHT_INDEX), batchMemoryManager.getTotalInputRecords(JoinBatchMemoryManager.RIGHT_INDEX));

    RecordBatchStats.logRecordBatchStats(getRecordBatchStatsContext(),
        "outgoing aggregate: batch count : %d, avg bytes : %d,  avg row bytes : %d, record count : %d",
        batchMemoryManager.getNumOutgoingBatches(), batchMemoryManager.getAvgOutputBatchSize(),
        batchMemoryManager.getAvgOutputRowWidth(), batchMemoryManager.getTotalOutputRecords());

    cleanup();
    super.close();
  }

  private HashExceptProbe setupHashJoinProbe() {
    // No real code generation
    return new HashExceptProbeTemplate();
  }

  @Override
  public void dump() {
    logger.error("HashJoinBatch[container={}, left={}, right={}, leftOutcome={}, rightOutcome={}, hashSetProbe={}," +
            " rightExpr={}, canSpill={}, buildSchema={}, probeSchema={}]", container, left, right, leftUpstream,
        rightUpstream, probe, rightExpr, canSpill, buildSchema, probeSchema);
  }
}