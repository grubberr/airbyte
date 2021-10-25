/*
 * Copyright (c) 2021 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.sync;

import io.airbyte.config.EnvConfigs;
import io.airbyte.config.NormalizationInput;
import io.airbyte.config.OperatorDbtInput;
import io.airbyte.config.StandardSyncInput;
import io.airbyte.config.StandardSyncOperation;
import io.airbyte.config.StandardSyncOperation.OperatorType;
import io.airbyte.config.StandardSyncOutput;
import io.airbyte.scheduler.models.IntegrationLauncherConfig;
import io.airbyte.scheduler.models.JobRunConfig;
import io.airbyte.workers.temporal.TemporalUtils;
import io.temporal.activity.ActivityCancellationType;
import io.temporal.activity.ActivityOptions;
import io.temporal.workflow.Workflow;
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SyncWorkflowImpl implements SyncWorkflow {

  private static final Logger LOGGER = LoggerFactory.getLogger(SyncWorkflowImpl.class);

  private static final int MAX_SYNC_TIMEOUT_DAYS = new EnvConfigs().getMaxSyncTimeoutDays();

  private static final ActivityOptions options = ActivityOptions.newBuilder()
      .setScheduleToCloseTimeout(Duration.ofDays(MAX_SYNC_TIMEOUT_DAYS))
      .setCancellationType(ActivityCancellationType.WAIT_CANCELLATION_COMPLETED)
      .setRetryOptions(TemporalUtils.NO_RETRY)
      .build();

  private final ReplicationActivity replicationActivity = Workflow.newActivityStub(ReplicationActivity.class, options);
  private final NormalizationActivity normalizationActivity = Workflow.newActivityStub(NormalizationActivity.class, options);
  private final DbtTransformationActivity dbtTransformationActivity = Workflow.newActivityStub(DbtTransformationActivity.class, options);

  @Override
  public StandardSyncOutput run(final JobRunConfig jobRunConfig,
                                final IntegrationLauncherConfig sourceLauncherConfig,
                                final IntegrationLauncherConfig destinationLauncherConfig,
                                final StandardSyncInput syncInput) {
    final StandardSyncOutput run = replicationActivity.replicate(jobRunConfig, sourceLauncherConfig, destinationLauncherConfig, syncInput);

    if (syncInput.getOperationSequence() != null && !syncInput.getOperationSequence().isEmpty()) {
      for (final StandardSyncOperation standardSyncOperation : syncInput.getOperationSequence()) {
        if (standardSyncOperation.getOperatorType() == OperatorType.NORMALIZATION) {
          final NormalizationInput normalizationInput = new NormalizationInput()
              .withDestinationConfiguration(syncInput.getDestinationConfiguration())
              .withCatalog(run.getOutputCatalog())
              .withResourceRequirements(syncInput.getResourceRequirements());

          normalizationActivity.normalize(jobRunConfig, destinationLauncherConfig, normalizationInput);
        } else if (standardSyncOperation.getOperatorType() == OperatorType.DBT) {
          final OperatorDbtInput operatorDbtInput = new OperatorDbtInput()
              .withDestinationConfiguration(syncInput.getDestinationConfiguration())
              .withOperatorDbt(standardSyncOperation.getOperatorDbt());

          dbtTransformationActivity.run(jobRunConfig, destinationLauncherConfig, syncInput.getResourceRequirements(), operatorDbtInput);
        } else {
          final String message = String.format("Unsupported operation type: %s", standardSyncOperation.getOperatorType());
          LOGGER.error(message);
          throw new IllegalArgumentException(message);
        }
      }
    }

    return run;
  }

}
