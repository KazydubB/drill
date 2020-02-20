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
import org.apache.drill.exec.planner.common.JoinControl;
import org.apache.drill.exec.planner.common.SetOperatorControl;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.RecordBatch.IterOutcome;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.drill.exec.vector.IntVector;
import org.apache.drill.exec.vector.ValueVector;

import static org.apache.drill.exec.record.JoinBatchMemoryManager.LEFT_INDEX;

public class HashSetProbeTemplate implements HashSetProbe {

  public static final int LEFT_INPUT = 0;
  public static final int RIGHT_INPUT = 1;

  VectorContainer container; // the outgoing container

  // Probe side record batch
  private RecordBatch probeBatch;

  private BatchSchema probeSchema;

  // Join type, INNER, LEFT, RIGHT or OUTER
  // private JoinRelType joinType;

  // joinControl determines how to handle INTERSECT_DISTINCT vs. INTERSECT_ALL
  @Deprecated
  private JoinControl joinControl;

  private SetOperatorControl control;

  private HashSetRecordBatch outgoingJoinBatch; // todo: rename

  // Number of records to process on the probe side
  private int recordsToProcess;

  // Number of records processed on the probe side
  private int recordsProcessed;

  // Number of records in the output container
  private int outputRecords;

  // Indicate if we should drain the next record from the probe side
  private boolean getNextRecord = true;

  // Contains both batch idx and record idx of the matching record in the build side
  private int currentCompositeIdx = -1;

  // Current state the hash join algorithm is in
  private ProbeState probeState; // = ProbeState.PROBE_EXCEPT;

  // For outer or right joins, this is a list of unmatched records that needs to be projected
  private IntArrayList unmatchedBuildIndexes;

  private HashPartition[] partitions;

  // While probing duplicates, retain current build-side partition in case need to continue
  // probing later on the same chain of duplicates
  private HashPartition currPartition;

  private int currRightPartition; // for returning RIGHT/FULL
  private IntVector readLeftHVVector; // HV vector that was read from the spilled batch
  private int cycleNum; // 1-primary, 2-secondary, 3-tertiary, etc.
  private HashSetRecordBatch.HashSetSpilledPartition[] spilledInners; // for the outer to find the partition
  private boolean buildSideIsEmpty = true;
  private int numPartitions = 1; // must be 2 to the power of bitsInMask
  private int partitionMask; // numPartitions - 1
  private int bitsInMask; // number of bits in the MASK
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
  public void setupHashSetProbe(RecordBatch probeBatch, HashSetRecordBatch outgoing, Type type,
                                IterOutcome leftStartState, HashPartition[] partitions, int cycleNum,
                                VectorContainer container, HashSetRecordBatch.HashSetSpilledPartition[] spilledInners,
                                boolean buildSideIsEmpty, int numPartitions, int rightHVColPosition) {
    this.container = container;
    this.spilledInners = spilledInners;
    this.probeBatch = probeBatch;
    this.probeSchema = probeBatch.getSchema();
    // this.joinType = joinRelType;
    this.outgoingJoinBatch = outgoing;
    this.partitions = partitions;
    this.cycleNum = cycleNum;
    this.buildSideIsEmpty = buildSideIsEmpty;
    this.numPartitions = numPartitions;
//    this.numberOfBuildSideColumns = semiJoin ? 0 : rightHVColPosition; // position (0 based) of added column == #columns
    this.numberOfBuildSideColumns = false ? 0 : rightHVColPosition; // position (0 based) of added column == #columns
//    this.semiJoin = semiJoin;

    partitionMask = numPartitions - 1; // e.g. 32 --> 0x1F
    bitsInMask = Integer.bitCount(partitionMask); // e.g. 0x1F -> 5
    // joinControl = new JoinControl(outgoingJoinBatch.getPopConfig().getJoinControl());

    control = new SetOperatorControl(SetOperatorControl.EXCEPT, outgoingJoinBatch.getPopConfig().isAll());

    // probeState = ProbeState.PROBE_PROJECT;
    this.recordsToProcess = 0;
    this.recordsProcessed = 0;

    // A special case - if the left was an empty file
    if (leftStartState == IterOutcome.NONE){
      changeToFinalProbeState();
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
      if (readLeftHVVector != null) { readLeftHVVector.clear();}
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
  private void appendBuild(VectorContainer buildSrcContainer, int buildSrcIndex) {
    for (int vectorIndex = 0; vectorIndex < numberOfBuildSideColumns; vectorIndex++) {
      ValueVector destVector = container.getValueVector(vectorIndex).getValueVector();
      ValueVector srcVector = buildSrcContainer.getValueVector(vectorIndex).getValueVector();
      destVector.copyEntry(container.getRecordCount(), srcVector, buildSrcIndex);
    }
  }

  /**
   * Append the given probe side row into the outgoing container, following the build side part
   * @param probeSrcContainer The container for the left/outer side
   * @param probeSrcIndex probe side index
   */
  private void appendProbe(VectorContainer probeSrcContainer, int probeSrcIndex) {
    for (int vectorIndex = numberOfBuildSideColumns; vectorIndex < container.getNumberOfColumns(); vectorIndex++) {
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
  private int outputRow(ArrayList<VectorContainer> buildSrcContainers, int compositeBuildSrcIndex,
                        VectorContainer probeSrcContainer, int probeSrcIndex) { // todo: change it to have proper output

    if (buildSrcContainers != null) {
      int buildBatchIndex = compositeBuildSrcIndex >>> 16;
      int buildOffset = compositeBuildSrcIndex & 65535;
      appendBuild(buildSrcContainers.get(buildBatchIndex), buildOffset);
    }
    if (probeSrcContainer != null) {
      appendProbe(probeSrcContainer, probeSrcIndex);
    }
    return container.incRecordCount();
  }

  /**
   * After the "inner" probe phase, finish up a Right (of Full) Join by projecting the unmatched rows of the build side
   * @param currBuildPart Which partition
   */
  private void executeProjectRightPhase(int currBuildPart) {
    while (outputRecords < targetOutputRecords && recordsProcessed < recordsToProcess) {
      outputRecords =
          outputRow(partitions[currBuildPart].getContainers(), unmatchedBuildIndexes.get(recordsProcessed),
              null /* no probeBatch */, 0 /* no probe index */);
      recordsProcessed++;
    }
  }

  private void executeProbePhase() throws SchemaChangeException {

//    while (outputRecords < targetOutputRecords && probeState != ProbeState.DONE && probeState != ProbeState.PROJECT_RIGHT) {
    while (outputRecords < targetOutputRecords && probeState != ProbeState.DONE && true /*probeState != ProbeState.PROJECT_RIGHT*/) { // todo: PROJECT_RIGHT should not be(?)

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
            changeToFinalProbeState();
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

//        System.out.println("getNextRecords=true");

        if (!buildSideIsEmpty) {
          int hashCode = (cycleNum == 0) ?
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

//        if (semiJoin) { // todo: actually, I need to treat it as semiJoin!
        if (false) {
          if (probeIndex != -1) {
            // output the probe side only
            outputRecords =
                outputRow(null, 0, probeBatch.getContainer(), recordsProcessed);
          }
          recordsProcessed++;
          continue; // no build-side duplicates, go on to the next probe-side row
        }


//        if (probeIndex != -1) { // todo: original
        if (probeIndex != -1 /*&& control.isIntersect()*/) {

          /* The current probe record has a key that matches. Get the index
           * of the first row in the build side that matches the current key
           * (and record this match in the bitmap, in case of a FULL/RIGHT join)
           */
          Pair<Integer, Boolean> matchStatus = currPartition.getStartIndex(probeIndex);
          boolean matchExists = matchStatus.getRight();

          System.out.println("matchExists=" + matchExists + ", " + (control.isExcept() ? "except" : "intersect") + " all=" + control.isAll());

          if (control.isIntersect()) {

//            /* The current probe record has a key that matches. Get the index
//             * of the first row in the build side that matches the current key
//             * (and record this match in the bitmap, in case of a FULL/RIGHT join)
//             */
//            Pair<Integer, Boolean> matchStatus = currPartition.getStartIndex(probeIndex);

//            boolean matchExists = matchStatus.getRight();

//            System.out.println("matchExists=" + matchExists);

//          if (joinControl.isIntersectDistinct() && matchExists) { // todo: implement some kind of SetOperatorControl to differentiate between EXCEPT and INTERSECT
//            // since it is intersect distinct and we already have one record matched, move to next probe row
//            recordsProcessed++;
//            continue;
//          }

            // todo: remove the next if
//            if (control.isExceptDistinct() && !matchExists) {
            if (control.isIntersectDistinct() && matchExists) {
              // since it is intersect distinct and we already have one record matched, move to next probe row
              recordsProcessed++;
              continue;
            }

            currentCompositeIdx = matchStatus.getLeft();

            // todo: shouldn't it happen in a case of except?
//            outputRecords = outputRow(currPartition.getContainers(), currentCompositeIdx, probeBatch.getContainer(), recordsProcessed);
            outputRecords = outputRow(null, 0, probeBatch.getContainer(), recordsProcessed);

            /* Projected single row from the build side with matching key but there
             * may be more rows with the same key. Check if that's the case as long as
             * we are not doing intersect distinct since it only cares about
             * distinct values.
             */
//          currentCompositeIdx = joinControl.isIntersectDistinct() ? -1 : // todo: I believe this is important
          currentCompositeIdx = control.isIntersectDistinct() ? -1 : // todo: I believe this is important
              currPartition.getNextIndex(currentCompositeIdx);

            // todo: this field seems to be very useful!
//            currentCompositeIdx = control.isExceptDistinct() ? -1 : currPartition.getNextIndex(currentCompositeIdx);

            if (currentCompositeIdx == -1) {
              /* We only had one row in the build side that matched the current key
               * from the probe side. Drain the next row in the probe side.
               */
              recordsProcessed++;
            } else {
              /* There is more than one row with the same key on the build side
               * don't drain more records from the probe side till we have projected
               * all the rows with this key
               */
              getNextRecord = false;
            }
          } else if (control.isExcept()) { // except

            if (control.isExceptDistinct()/* && matchExists*/) { // todo: match in a sense if it was already output. Must do so?
              // since it is except distinct and we already have one record matched, move to next probe row
              recordsProcessed++;
              continue;
            }


            recordsProcessed++;
            getNextRecord = true; // todo: can be discarded
            // currentCompositeIdx // todo: handlet this?
          }
        } else { // No matching key // todo: this seems to be the place to handle except; change the comment

          if (control.isExcept()) {
            // todo: something like matchExists should be introduced (as in the case above)
            // todo: I assume this may be possible by having Set with build indexes
//            if (control.isExceptDistinct() && ) // todo: do not allow duplicates

            System.out.println("Except triggered!");

            outputRecords = // output only the probe side (the build side would be all nulls)
                outputRow(null, 0, probeBatch.getContainer(), recordsProcessed);
          }

          // If we have a left outer join, project the outer side
//          if (joinType == JoinRelType.LEFT || joinType == JoinRelType.FULL) {
//          if (true/*joinType == JoinRelType.LEFT || joinType == JoinRelType.FULL*/) { // todo: I assume this is going to be LEFT
//          if (probeIndex != -1) { // the key exists // todo: remove
//
//            outputRecords = // output only the probe side (the build side would be all nulls)
//                outputRow(null, 0, probeBatch.getContainer(), recordsProcessed);
//          }
          recordsProcessed++;
        }
      } else { // match the next inner row with the same key

        System.out.println("getNextRecords=false. Should the code even reach there?");

        currPartition.setRecordMatched(currentCompositeIdx);

        outputRecords =
            outputRow(currPartition.getContainers(), currentCompositeIdx,
                probeBatch.getContainer(), recordsProcessed);

        currentCompositeIdx = currPartition.getNextIndex(currentCompositeIdx);

        if (currentCompositeIdx == -1) {
          // We don't have any more rows matching the current key on the build side, move on to the next probe row
          getNextRecord = true;
          recordsProcessed++;
        } else {
          recordsProcessed++;
        }
      }

      System.out.println("recordsProcessed=" + recordsProcessed);
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
    if (probeState == ProbeState.DONE) {
      return outputRecords; // that is zero
    }

//    if (probeState == ProbeState.PROBE_PROJECT) {
    if (true/*probeState == ProbeState.PROBE_PROJECT*/) { // todo: should be true?
      executeProbePhase();
    }

//    if (probeState == ProbeState.PROJECT_RIGHT) {
    if (false/*probeState == ProbeState.PROJECT_RIGHT*/) { // todo: PROJECT_RIGHT should not be true
      // Inner probe is done; now we are here because we still have a RIGHT OUTER (or a FULL) join

      do {

        if (unmatchedBuildIndexes == null) { // first time for this partition ?
          if (buildSideIsEmpty) { return outputRecords; } // in case of an empty right
          // Get this partition's list of build indexes that didn't match any record on the probe side
          unmatchedBuildIndexes = partitions[currRightPartition].getNextUnmatchedIndex();
          recordsProcessed = 0;
          recordsToProcess = unmatchedBuildIndexes.size();
        }

        // Project the list of unmatched records on the build side
        executeProjectRightPhase(currRightPartition);

        if (recordsProcessed < recordsToProcess) { // more records in this partition?
          return outputRecords;  // outgoing is full; report and come back later
        } else {
          currRightPartition++; // on to the next right partition
          unmatchedBuildIndexes = null;
        }

      }   while (currRightPartition < numPartitions);

      probeState = ProbeState.DONE; // last right partition was handled; we are done now
    }

    return outputRecords;
  }

  @Override
  public void changeToFinalProbeState() {

    System.out.println("HashSetProbeTemplate#changeToFinalProbeState");

    System.out.println("outputRecords=" + outputRecords);

    probeState = ProbeState.DONE;

    // We are done with the (left) probe phase.
    // If it's a RIGHT or a FULL join then need to get the unmatched indexes from the build side
    // todo: probably implement something similar
//    probeState =
//        (joinType == JoinRelType.RIGHT || joinType == JoinRelType.FULL) ? ProbeState.PROJECT_RIGHT :
//            ProbeState.DONE; // else we're done
  }

  @Override
  public String toString() {
    return "HashJoinProbeTemplate[container=" + container
        + ", probeSchema=" + probeSchema
//        + ", type=" + type
        + ", recordsToProcess=" + recordsToProcess
        + ", recordsProcessed=" + recordsProcessed
        + ", outputRecords=" + outputRecords
        + ", probeState=" + probeState
        + ", unmatchedBuildIndexes=" + unmatchedBuildIndexes
        + "]";
  }
}
