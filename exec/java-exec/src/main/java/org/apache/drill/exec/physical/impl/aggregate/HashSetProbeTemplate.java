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
package org.apache.drill.exec.physical.impl.aggregate;

import java.util.ArrayList;

import com.carrotsearch.hppc.IntArrayList;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.physical.impl.common.HashPartition;
import org.apache.drill.exec.physical.impl.set.HashSetRecordBatch;
import org.apache.drill.exec.planner.common.SetOperatorControl;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.RecordBatch.IterOutcome;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.vector.IntVector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.commons.lang3.tuple.Pair;

import static org.apache.drill.exec.record.JoinBatchMemoryManager.LEFT_INDEX;

public class HashSetProbeTemplate implements HashSetProbe {

  public static final int LEFT_INPUT = 0;
  public static final int RIGHT_INPUT = 1;

  VectorContainer container; // the outgoing container

  // Probe side record batch
  private RecordBatch probeBatch; // todo: do not modify its container

  private BatchSchema probeSchema;

  // Join type, INNER, LEFT, RIGHT or OUTER
  // private JoinRelType joinType;

  // joinControl determines how to handle INTERSECT_DISTINCT vs. INTERSECT_ALL
//  @Deprecated
//  private JoinControl joinControl;

  protected SetOperatorControl operatorControl;

  protected HashSetRecordBatch outgoingJoinBatch; // todo: rename

  // Number of records to process on the probe side
  protected int recordsToProcess;

  // Number of records processed on the probe side
  protected int recordsProcessed;

  // Number of records in the output container
  protected int outputRecords;

  // Indicate if we should drain the next record from the probe side
  protected boolean getNextRecord = true;

  // Contains both batch idx and record idx of the matching record in the build side
  protected int currentCompositeIdx = -1;

  // Current state the hash join algorithm is in
//  private ProbeState probeState; // = ProbeState.PROBE_EXCEPT;
  private boolean isDone;

  // For outer or right joins, this is a list of unmatched records that needs to be projected
  private IntArrayList unmatchedBuildIndexes; // todo: actually, outer join should be the behaviour I need for EXCEPT DISTINCT?

  @Deprecated // todo:?
  private HashPartition[] partitions;

  // While probing duplicates, retain current build-side partition in case need to continue
  // probing later on the same chain of duplicates
  private HashPartition currPartition; // todo: this may be useful!

  @Deprecated
  private int currRightPartition; // for returning RIGHT/FULL
  private IntVector readLeftHVVector; // HV vector that was read from the spilled batch
  private int cycleNum; // 1-primary, 2-secondary, 3-tertiary, etc.
  private HashSetRecordBatch.HashSetSpilledPartition[] spilledInners; // for the outer to find the partition
  private boolean buildSideIsEmpty = true;
  private int numPartitions = 1; // must be 2 to the power of bitsInMask
  private int partitionMask; // numPartitions - 1
  private int bitsInMask; // number of bits in the MASK
  @Deprecated
  private int numberOfBuildSideColumns;
  private int targetOutputRecords;
//  private boolean semiJoin;

  @Override
  public void setTargetOutputCount(int targetOutputRecords) {
    this.targetOutputRecords = targetOutputRecords;
  }

  @Override
  public int getOutputCount() {
    return outputRecords;
  }

  /**
   *  Setup the Hash Join Probe object
   *
   * @param probeBatch
   * @param outgoing
   * @param leftStartState
   * @param partitions
   * @param cycleNum
   * @param container
   * @param spilledInners
   * @param buildSideIsEmpty
   * @param numPartitions
   * @param rightHVColPosition
   */
  @Override
  public void setupHashSetProbe(RecordBatch probeBatch, HashSetRecordBatch outgoing, SetOperatorControl operatorControl/*Type type*/, // todo: change Type to operatorControl
                                IterOutcome leftStartState, HashPartition[] partitions, int cycleNum,
                                VectorContainer container, HashSetRecordBatch.HashSetSpilledPartition[] spilledInners,
                                boolean buildSideIsEmpty, int numPartitions, int rightHVColPosition) {
    this.container = container;
    this.spilledInners = spilledInners;
    this.probeBatch = probeBatch;
    this.probeSchema = probeBatch.getSchema();
    this.outgoingJoinBatch = outgoing;
    this.partitions = partitions;
    this.cycleNum = cycleNum;
    this.buildSideIsEmpty = buildSideIsEmpty;
    this.numPartitions = numPartitions;
    this.numberOfBuildSideColumns = true ? 0 : rightHVColPosition; // position (0 based) of added column == #columns

    partitionMask = numPartitions - 1; // e.g. 32 --> 0x1F
    bitsInMask = Integer.bitCount(partitionMask); // e.g. 0x1F -> 5

//    control = SetOperatorControl.except(outgoingJoinBatch.getPopConfig().isAll());
    this.operatorControl = operatorControl;

    // probeState = ProbeState.PROBE_PROJECT;
    this.recordsToProcess = 0;
    this.recordsProcessed = 0;

    // A special case - if the left was an empty file
    if (leftStartState == IterOutcome.NONE){
      markAsDone();
    } else {
      this.recordsToProcess = probeBatch.getRecordCount();
    }

    // for those outer partitions that need spilling (cause their matching inners spilled)
    // initialize those partitions' current batches and hash-value vectors
    for (HashPartition partition : this.partitions) {
      partition.allocateNewCurrentBatchAndHV();
    }

    currRightPartition = 0; // In case it's a Right/Full outer join

    // Initialize the HV vector for the first (already read) left batch
    if (this.cycleNum > 0) {
      if (readLeftHVVector != null) {
        readLeftHVVector.clear();
      }
      if (leftStartState != IterOutcome.NONE) { // Skip when outer spill was empty
        readLeftHVVector = (IntVector) probeBatch.getContainer().getLast();
      }
    }
  }

  /**
   * Append the given build side row into the outgoing container
   * @param buildSrcContainer The container for the right/inner side
   * @param buildSrcIndex build side index
   */
  @Deprecated // todo: remove?
  private void appendBuild(VectorContainer buildSrcContainer, int buildSrcIndex) {
    for (int vectorIndex = 0; vectorIndex < numberOfBuildSideColumns; vectorIndex++) {
      ValueVector destVector = container.getValueVector(vectorIndex).getValueVector();
      ValueVector srcVector = buildSrcContainer.getValueVector(vectorIndex).getValueVector();
      destVector.copyEntry(container.getRecordCount(), srcVector, buildSrcIndex);
    }
  }

  /**todo:
   * Append the given probe side row into the outgoing container, following the build side part
   * @param probeSrcContainer The container for the left/outer side
   * @param probeSrcIndex probe side index
   */
  private void appendProbe(VectorContainer probeSrcContainer, int probeSrcIndex) {
    for (int vectorIndex = numberOfBuildSideColumns; vectorIndex < container.getNumberOfColumns(); vectorIndex++) { // todo: notice change in number of columns
//    for (int vectorIndex = numberOfBuildSideColumns; vectorIndex < probeSrcContainer.getNumberOfColumns(); vectorIndex++) {
      ValueVector destVector = container.getValueVector(vectorIndex).getValueVector();
      ValueVector srcVector = probeSrcContainer.getValueVector(vectorIndex - numberOfBuildSideColumns).getValueVector();
      destVector.copyEntry(container.getRecordCount(), srcVector, probeSrcIndex);
    }
  }

  /**
   *  A special version of the VectorContainer's appendRow for the HashJoin; (following a probe) it
   *  copies the build and probe sides into the outgoing container. (It uses a composite
   *  index for the build side). If any of the build/probe source containers is null, then that side
   *  is not appended (effectively outputing nulls for that side's columns).
   * @param buildSrcContainers The containers list for the right/inner side
   * @param compositeBuildSrcIndex Composite build index
   * @param probeSrcContainer The single container for the left/outer side
   * @param probeSrcIndex Index in the outer container
   * @return Number of rows in this container (after the append)
   */
  @Deprecated // todo:
  private int outputRow(ArrayList<VectorContainer> buildSrcContainers, int compositeBuildSrcIndex,
                        VectorContainer probeSrcContainer, int probeSrcIndex) { // todo: change it to have proper output

    if (buildSrcContainers != null) {
      int buildBatchIndex = compositeBuildSrcIndex >>> 16;
      int buildOffset = compositeBuildSrcIndex & 65535;
      appendProbe(buildSrcContainers.get(buildBatchIndex), buildOffset); // todo: this was switched with the line above
    }
    if (probeSrcContainer != null) {
      appendProbe(probeSrcContainer, probeSrcIndex);
    }
    return container.incRecordCount();
  }

  private int outputRow(VectorContainer probeSrcContainer, int probeSrcIndex) { // todo: change it to have proper output
    if (probeSrcContainer != null) {
      appendProbe(probeSrcContainer, probeSrcIndex);
    }
    return container.incRecordCount();
  }

  private void executeProbePhase() throws SchemaChangeException {

    while (outputRecords < targetOutputRecords && !isDone) {

      // Check if we have processed all records in this batch we need to invoke next
      if (recordsProcessed == recordsToProcess) {

        // Done processing all records in the previous batch, clean up!
        for (VectorWrapper<?> wrapper : probeBatch) {
          wrapper.getValueVector().clear();
        }

//        IterOutcome leftUpstream = outgoingJoinBatch.next(HashJoinHelper.LEFT_INPUT, probeBatch);
        IterOutcome leftUpstream = outgoingJoinBatch.next(LEFT_INPUT, probeBatch);

        switch (leftUpstream) {
          case NONE:
          case NOT_YET:
          case STOP:
            recordsProcessed = 0;
            recordsToProcess = 0;
            markAsDone();
//            isDone = true;
            // in case some outer partitions were spilled, need to spill their last batches
            for (HashPartition partition : partitions) {
              if (!partition.isSpilled()) { // skip non-spilled
                continue;
              }
              partition.completeAnOuterBatch(false);
              // update the partition's spill record with the outer side
              HashSetRecordBatch.HashSetSpilledPartition sp = spilledInners[partition.getPartitionNum()];
              sp.updateOuter(partition.getPartitionBatchesCount(), partition.getSpillFile());

              partition.closeWriter();
            }

            continue;

          case OK_NEW_SCHEMA:
            if (probeBatch.getSchema().equals(probeSchema)) {
              for (HashPartition partition : partitions) {
                partition.updateBatches();
              }
            } else {
              throw SchemaChangeException.schemaChanged("Hash join does not support schema changes in probe side.",
                  probeSchema, probeBatch.getSchema());
            }
          case OK:
            outgoingJoinBatch.getBatchMemoryManager().update(probeBatch, LEFT_INDEX, outputRecords);
            setTargetOutputCount(outgoingJoinBatch.getBatchMemoryManager().getCurrentOutgoingMaxRowCount()); // calculated by update()
            recordsToProcess = probeBatch.getRecordCount();
            recordsProcessed = 0;
            // If we received an empty batch do nothing
            if (recordsToProcess == 0) {
              continue;
            }
            if (cycleNum > 0) {
              readLeftHVVector = (IntVector) probeBatch.getContainer().getLast(); // Needed ?
            }
        }
      }

      int probeIndex = -1;
      // Check if we need to drain the next row in the probe side
      if (getNextRecord) {

        int hashCode = -1;
        if (!buildSideIsEmpty) { // todo: must handle the case when build side is empty and EXCEPT DISTINCT...
          hashCode = (cycleNum == 0) ?
              partitions[0].getProbeHashCode(recordsProcessed)
              : readLeftHVVector.getAccessor().get(recordsProcessed);
          int currBuildPart = hashCode & partitionMask;
          hashCode >>>= bitsInMask;

          // Set and keep the current partition (may be used again on subsequent probe calls as
          // inner rows of duplicate key are processed)
          currPartition = partitions[currBuildPart]; // inner if not spilled, else outer

          // If the matching inner partition was spilled
          if (outgoingJoinBatch.isSpilledInner(currBuildPart)) {
            // add this row to its outer partition (may cause a spill, when the batch is full)

            currPartition.appendOuterRow(hashCode, recordsProcessed);

            recordsProcessed++; // done with this outer record
            continue; // on to the next outer record
          }

          probeIndex = currPartition.probeForKey(recordsProcessed, hashCode);

        }

        if (probeIndex != -1) {

          // Record this match in the bitmap
          currPartition.getStartIndex(probeIndex);

          recordsProcessed++;
        } else { // No matching key // todo: this seems to be the place to handle except; change the comment

          if (operatorControl.isExcept() && operatorControl.isAll()) {
            outputRecords = // output only the probe side (the build side would be all nulls) // todo: change comment
                outputRow(probeBatch.getContainer(), recordsProcessed);
          }

          recordsProcessed++; // todo: should this be not incremented when EXCEPT DISTINCT?
        }
      } else { // match the next inner row with the same key

        System.out.println("getNextRecords=false. Should the code even reach there?");

        currPartition.setRecordMatched(currentCompositeIdx);

        outputRecords =
            outputRow(// currPartition.getContainers(), currentCompositeIdx,
                probeBatch.getContainer(), recordsProcessed);

        currentCompositeIdx = currPartition.getNextIndex(currentCompositeIdx);

        if (currentCompositeIdx == -1) {
          // We don't have any more rows matching the current key on the build side, move on to the next probe row
          getNextRecord = true;
//          recordsProcessed++;
        } /*else {
          recordsProcessed++;
        }*/
        recordsProcessed++;
      }

//      System.out.println("recordsProcessed=" + recordsProcessed);
    }
  }

  /**
   *  Perform the probe, till the outgoing is full, or no more rows to probe.
   *  Performs the inner or left-outer join while there are left rows,
   *  when done, continue with right-outer, if appropriate.
   * @return Num of output records
   * @throws SchemaChangeException
   */
  @Override
  public int probeAndProject() throws SchemaChangeException {

    outputRecords = 0;

    // When handling spilled partitions, the state becomes DONE at the end of each partition
    if (isDone) {
      return outputRecords; // that is zero
    }

    executeProbePhase();

    if (operatorControl.isExcept() && (operatorControl.isDistinct() /*|| operatorControl.isAll()*/)) {
      // Inner probe is done; now we are here because we still have a RIGHT OUTER (or a FULL) join

      do {

        if (unmatchedBuildIndexes == null) { // first time for this partition ?
          if (buildSideIsEmpty) { // in case of an empty right // todo: or left?
            return outputRecords;
          }
          // Get this partition's list of build indexes that didn't match any record on the probe side
          unmatchedBuildIndexes = operatorControl.isDistinct() ? partitions[currRightPartition].getNextDistinctUnmatchedIndex() : partitions[currRightPartition].getNextUnmatchedIndex();
          recordsProcessed = 0;
          recordsToProcess = unmatchedBuildIndexes.size();
        }

        // Project the list of unmatched records on the build side

        // assert false;
        executeProjectRightPhase(currRightPartition);

        if (recordsProcessed < recordsToProcess) { // more records in this partition?
          return outputRecords;  // outgoing is full; report and come back later
        } else {
          currRightPartition++; // on to the next right partition
          unmatchedBuildIndexes = null;
        }

      } while (currRightPartition < numPartitions);

      isDone = true; // last right partition was handled; we are done now
    }

    return outputRecords;
  }

  /**
   * After the "inner" probe phase, finish up a Right (of Full) Join by projecting the unmatched rows of the build side
   * @param currBuildPart Which partition
   */
  private void executeProjectRightPhase(int currBuildPart) { // todo: this might be useful actually
    assert operatorControl.isDistinct();
    while (outputRecords < targetOutputRecords && recordsProcessed < recordsToProcess) {
      int compositeBuildSrcIndex = unmatchedBuildIndexes.get(recordsProcessed);
      int buildBatchIndex = compositeBuildSrcIndex >>> 16;
      int buildOffset = compositeBuildSrcIndex & 65535;
      VectorContainer container = partitions[currBuildPart].getContainers().get(buildBatchIndex);
      outputRecords = outputRow(container, buildOffset);
//          outputRow(partitions[currBuildPart].getContainers(), unmatchedBuildIndexes.get(recordsProcessed),
//              null /* no probeBatch */, 0 /* no probe index */);
      recordsProcessed++;
    }

    System.out.println("\n\n\n\n--------------\nunmatchedBuildIndexes: " + unmatchedBuildIndexes + "\n-------------------------\n\n\n\n");
  }

  @Override
  public void markAsDone() {
    isDone = true;
  }

  @Override
  public String toString() {
    return "HashJoinProbeTemplate[container=" + container
        + ", probeSchema=" + probeSchema
        + ", recordsToProcess=" + recordsToProcess
        + ", recordsProcessed=" + recordsProcessed
        + ", outputRecords=" + outputRecords
        + ", isDone=" + isDone
        + ", unmatchedBuildIndexes=" + unmatchedBuildIndexes
        + "]";
  }
}
