#!/usr/bin/env bash
# Run all @FuzzTest cases over the KafkaApis fuzz targets with the JaCoCo
# agent attached, then render an HTML/XML/CSV coverage report.
#
# See ./README.md for design notes (why JAVA_TOOL_OPTIONS, why one Gradle
# invocation per fuzz test, etc.).
#
# Modes (selectable via the FUZZ_MODE env var):
#
#   fresh     [default] start with an empty coverage.exec and run all
#             fuzz tests from scratch.
#   resume    seed coverage.exec from the committed snapshot
#             ./coverage_results/coverage.exec.xz (xz-compressed,
#             ~500 KB) and append today's runs on top of it. Useful
#             when the previous fuzz pass already cost ~50 minutes
#             and you only want to add incremental coverage.
#   snapshot  same as fresh, but at the very end re-compresses the
#             produced coverage.exec back into
#             ./coverage_results/coverage.exec.xz so the in-tree
#             snapshot stays in sync. Use this whenever you commit
#             a new run.
#
# Output:
#   /tmp/jacoco/coverage.exec            JaCoCo execution data (binary)
#   /tmp/jacoco/report/html/index.html   HTML coverage report
#   /tmp/jacoco/report/coverage.xml      XML coverage report (per-method)
#   /tmp/jacoco/report/coverage.csv      CSV coverage report (per-class)
#
# Optional: `FUZZ_SUITE=all` (default) runs every fuzz target; set
# `FUZZ_SUITE=offset-fetch` to run only `HandleOffsetFetchRequestFuzzTest`, or
# `FUZZ_SUITE=describe-configs` for `HandleDescribeConfigsRequestFuzzTest`, or
# `FUZZ_SUITE=describe-log-dirs` for `HandleDescribeLogDirsRequestFuzzTest`, or
# `FUZZ_SUITE=sasl-authenticate` for `HandleSaslAuthenticateRequestFuzzTest`, or
# `FUZZ_SUITE=sasl-handshake` for `HandleSaslHandshakeRequestFuzzTest`, or
# `FUZZ_SUITE=alter-replica-log-dirs` for `HandleAlterReplicaLogDirsRequestFuzzTest`, or
# `FUZZ_SUITE=create-partitions` for `HandleCreatePartitionsRequestFuzzTest`, or
# `FUZZ_SUITE=create-delegation-token` for `HandleCreateTokenRequestFuzzTest`, or
# `FUZZ_SUITE=renew-delegation-token` for `HandleRenewTokenRequestFuzzTest`, or
# `FUZZ_SUITE=expire-delegation-token` for `HandleExpireTokenRequestFuzzTest`, or
# `FUZZ_SUITE=describe-delegation-token` for `HandleDescribeTokensRequestFuzzTest`, or
# `FUZZ_SUITE=delete-groups` for `HandleDeleteGroupsRequestFuzzTest`, or
# `FUZZ_SUITE=get-telemetry-subscriptions` for `HandleGetTelemetrySubscriptionsRequestFuzzTest`, or
# `FUZZ_SUITE=push-telemetry` for `HandlePushTelemetryRequestFuzzTest`.
# (useful with `FUZZ_MODE=resume` to append coverage for the new tests only).
#
set -u

REPO_ROOT="$(cd "$(dirname "$0")/../../../.." && pwd)"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
COMMITTED_EXEC_XZ="$SCRIPT_DIR/coverage_results/coverage.exec.xz"
cd "$REPO_ROOT"

FUZZ_MODE="${FUZZ_MODE:-fresh}"
case "$FUZZ_MODE" in
    fresh|resume|snapshot) ;;
    *)
        echo "[fuzz] unknown FUZZ_MODE=$FUZZ_MODE (expected: fresh|resume|snapshot)" >&2
        exit 2
        ;;
esac

# Optional: run a subset of fuzz tests (default: full suite). Use with
# FUZZ_MODE=resume to append only new targets onto the committed snapshot.
#   FUZZ_SUITE=all               full suite
#   FUZZ_SUITE=offset-fetch      HandleOffsetFetchRequestFuzzTest only
#   FUZZ_SUITE=describe-configs       HandleDescribeConfigsRequestFuzzTest only
#   FUZZ_SUITE=describe-log-dirs      HandleDescribeLogDirsRequestFuzzTest only
#   FUZZ_SUITE=sasl-authenticate      HandleSaslAuthenticateRequestFuzzTest only
#   FUZZ_SUITE=sasl-handshake         HandleSaslHandshakeRequestFuzzTest only
#   FUZZ_SUITE=alter-replica-log-dirs HandleAlterReplicaLogDirsRequestFuzzTest only
#   FUZZ_SUITE=create-partitions      HandleCreatePartitionsRequestFuzzTest only
#   FUZZ_SUITE=create-delegation-token HandleCreateTokenRequestFuzzTest only
#   FUZZ_SUITE=renew-delegation-token   HandleRenewTokenRequestFuzzTest only
#   FUZZ_SUITE=expire-delegation-token  HandleExpireTokenRequestFuzzTest only
#   FUZZ_SUITE=describe-delegation-token HandleDescribeTokensRequestFuzzTest only
#   FUZZ_SUITE=delete-groups           HandleDeleteGroupsRequestFuzzTest only
#   FUZZ_SUITE=get-telemetry-subscriptions HandleGetTelemetrySubscriptionsRequestFuzzTest only
#   FUZZ_SUITE=push-telemetry HandlePushTelemetryRequestFuzzTest only
FUZZ_SUITE="${FUZZ_SUITE:-all}"
case "$FUZZ_SUITE" in
    all|offset-fetch|describe-configs|describe-log-dirs|sasl-authenticate|sasl-handshake|alter-replica-log-dirs|create-partitions|create-delegation-token|renew-delegation-token|expire-delegation-token|describe-delegation-token|delete-groups|get-telemetry-subscriptions|push-telemetry) ;;
    *)
        echo "[fuzz] unknown FUZZ_SUITE=$FUZZ_SUITE (expected: all|offset-fetch|describe-configs|describe-log-dirs|sasl-authenticate|sasl-handshake|alter-replica-log-dirs|create-partitions|create-delegation-token|renew-delegation-token|expire-delegation-token|describe-delegation-token|delete-groups|get-telemetry-subscriptions|push-telemetry)" >&2
        exit 2
        ;;
esac

JACOCO_VERSION="${JACOCO_VERSION:-0.8.12}"
JACOCO_DIR="/tmp/jacoco"
AGENT="$JACOCO_DIR/lib/jacocoagent.jar"
CLI="$JACOCO_DIR/lib/jacococli.jar"
EXEC="$JACOCO_DIR/coverage.exec"
REPORT_DIR="$JACOCO_DIR/report"

# 1. Fetch JaCoCo if not already present.
if [[ ! -f "$AGENT" || ! -f "$CLI" ]]; then
    mkdir -p "$JACOCO_DIR"
    cd "$JACOCO_DIR"
    echo "[fuzz] downloading JaCoCo $JACOCO_VERSION..."
    curl -fsSLo jacoco.zip \
        "https://repo1.maven.org/maven2/org/jacoco/jacoco/${JACOCO_VERSION}/jacoco-${JACOCO_VERSION}.zip"
    unzip -q -o jacoco.zip
    cd "$REPO_ROOT"
fi

# 1b. Either start fresh or seed from the committed snapshot.
if [[ "$FUZZ_MODE" == "resume" ]]; then
    if [[ ! -f "$COMMITTED_EXEC_XZ" ]]; then
        echo "[fuzz] FUZZ_MODE=resume but $COMMITTED_EXEC_XZ does not exist" >&2
        exit 2
    fi
    echo "[fuzz] resuming from $COMMITTED_EXEC_XZ"
    mkdir -p "$JACOCO_DIR"
    xz -dc "$COMMITTED_EXEC_XZ" > "$EXEC"
else
    rm -f "$EXEC"
fi

# 2. Attach the JaCoCo agent to every JVM the JDK launches (including
#    Gradle's forked test JVMs) via JAVA_TOOL_OPTIONS, and tell jazzer-junit
#    to actually fuzz.
export JAVA_TOOL_OPTIONS="-javaagent:${AGENT}=destfile=${EXEC},append=true,\
excludes=*junit*:*mockito.*:com.code_intelligence.*:org.jacoco.*"
export JAZZER_FUZZ=1

# 3. Run each fuzz test in its own JVM (Jazzer fuzzes only the first
#    @FuzzTest per JVM lifetime).
# handleProduceRequest fuzz targets (HandleProduceRequestFuzzTest)
PRODUCE_TESTS=(
    # Original tests (maxDuration = 100s each)
    fuzzTestProduceResponseContainsNewLeaderOnNotLeaderOrFollower
    fuzzTestTransactionalParametersSetCorrectly
    fuzzTestNullableTransactionalId
    fuzzTestNoAuthorizedTransactionalRequest
    fuzzTestNoAuthorized
    # Coverage-improvement tests (maxDuration = 20s each)
    fuzzTestUnknownTopicOrPartition
    fuzzTestThrottlingAndAckZeroNoOp
    fuzzTestRequestThrottleDominates
)

# handleFetchRequest fuzz targets (HandleFetchRequestFuzzTest, maxDuration = 20s each)
FETCH_TESTS=(
    fuzzTestFetchConsumer
    fuzzTestFetchFollower
    fuzzTestFetchThrottling
    fuzzTestFetchEmptyInteresting
    fuzzTestFetchDownConversion
)

# handleDescribeTopicPartitionsRequest fuzz targets (ZK arm only;
# HandleDescribeTopicPartitionsRequestFuzzTest, maxDuration = 20s each)
DESCRIBE_TP_TESTS=(
    fuzzTestZkUnsupportedVersion
    fuzzTestZkUnsupportedVersionThrottled
    fuzzTestZkUnsupportedVersionForwarded
)

# handleHeartbeatRequest fuzz targets (HandleHeartbeatRequestFuzzTest,
# maxDuration = 20s each)
HEARTBEAT_TESTS=(
    fuzzTestHeartbeatCoordinatorPath
    fuzzTestHeartbeatStaticMembershipOldIbp
    fuzzTestHeartbeatStaticMembershipSupportedIbp
    fuzzTestHeartbeatAuthorizationDenied
    fuzzTestHeartbeatThrottled
)

# handleOffsetFetchRequest fuzz targets (HandleOffsetFetchRequestFuzzTest,
# maxDuration = 20s each)
OFFSET_FETCH_TESTS=(
    fuzzTestOffsetFetchZk
    fuzzTestOffsetFetchCoordinatorMultiGroup
    fuzzTestOffsetFetchCoordinatorV1To7
    fuzzTestOffsetFetchCoordinatorThrottleAndForwarded
    fuzzTestOffsetFetchCoordinatorAuthAndHandleExceptions
)

# handleSyncGroupRequest fuzz targets (HandleSyncGroupRequestFuzzTest,
# maxDuration = 10s each, matching KafkaApisTest.FUZZ_DURATION)
SYNC_GROUP_TESTS=(
    fuzzTestSyncGroupStaticMembershipOldIbp
    fuzzTestSyncGroupInconsistentProtocol
    fuzzTestSyncGroupAuthorizationDenied
    fuzzTestSyncGroupCoordinatorFuture
    fuzzTestSyncGroupStaticMembershipSupportedIbp
    fuzzTestSyncGroupThrottledResponse
)

# handleLeaveGroupRequest fuzz targets (HandleLeaveGroupRequestFuzzTest,
# maxDuration = 10s each, matching KafkaApisTest.FUZZ_DURATION)
LEAVE_GROUP_TESTS=(
    fuzzTestLeaveGroupAuthorizationDenied
    fuzzTestLeaveGroupCoordinatorFuture
    fuzzTestLeaveGroupThrottledResponse
)

# handleDeleteGroupsRequest fuzz targets (HandleDeleteGroupsRequestFuzzTest,
# maxDuration = 10s each, matching KafkaApisTest.FUZZ_DURATION)
DELETE_GROUPS_TESTS=(
    fuzzTestDeleteGroupsAuthorizedGroupIdNotFound
    fuzzTestDeleteGroupsAllUnauthorized
    fuzzTestDeleteGroupsMixedAuthGroupIdNotFound
    fuzzTestDeleteGroupsDuplicateGroupIds
    fuzzTestDeleteGroupsNotCoordinatorOtherPartition
    fuzzTestDeleteGroupsEmptyGroupList
    fuzzTestDeleteGroupsCoordinatorFutureFailed
    fuzzTestDeleteGroupsThrottledResponse
    fuzzTestDeleteGroupsForwardedInnerRequest
)

# handleDescribeGroupsRequest fuzz targets (HandleDescribeGroupsRequestFuzzTest,
# maxDuration = 10s each, matching KafkaApisTest.FUZZ_DURATION)
DESCRIBE_GROUPS_TESTS=(
    fuzzTestDescribeGroupsMixedAuthorization
    fuzzTestDescribeGroupsAllGroupsUnauthorized
    fuzzTestDescribeGroupsCoordinatorException
    fuzzTestDescribeGroupsSuccessWithAuthorizedOperations
    fuzzTestDescribeGroupsThrottledResponse
)

# handleDescribeAcls / AclApis.handleDescribeAcls fuzz targets (HandleDescribeAclsRequestFuzzTest,
# maxDuration = 10s each, matching KafkaApisTest.FUZZ_DURATION)
DESCRIBE_ACLS_TESTS=(
    fuzzTestDescribeAclsSecurityDisabledNoAuthorizer
    fuzzTestDescribeAclsClusterDescribeDenied
    fuzzTestDescribeAclsAuthorizerReturnsEmpty
    fuzzTestDescribeAclsAuthorizerReturnsBindings
    fuzzTestDescribeAclsAuthorizerReturnsClusterBinding
    fuzzTestDescribeAclsThrottledResponse
    fuzzTestDescribeAclsForwardedInnerRequest
    fuzzTestDescribeAclsWireVersion0PatternAnyNormalized
    fuzzTestDescribeAclsWireVersion0NonLiteralPatternThrows
    fuzzTestDescribeAclsUnknownFilterElementsThrows
    fuzzTestDescribeAclsMixedWireVersionLiteralFilter
)

# handleListGroupsRequest fuzz targets (HandleListGroupsRequestFuzzTest,
# maxDuration = 10s each, matching KafkaApisTest.FUZZ_DURATION)
LIST_GROUPS_TESTS=(
    fuzzTestListGroupsPassthroughWithoutAuthorizer
    fuzzTestListGroupsFilteredWhenClusterDescribeDenied
    fuzzTestListGroupsCoordinatorException
    fuzzTestListGroupsThrottledResponse
)

# handleApiVersionsRequest fuzz targets (HandleApiVersionsRequestFuzzTest,
# maxDuration = 10s each, matching KafkaApisTest.FUZZ_DURATION)
API_VERSIONS_TESTS=(
    fuzzTestApiVersionsSuccessPath
    fuzzTestApiVersionsInvalidClientSoftware
    fuzzTestApiVersionsUnsupportedWireHeader
    fuzzTestApiVersionsThrottledResponse
    fuzzTestApiVersionsForwardedInnerRequest
)

# handleDeleteRecordsRequest fuzz targets (HandleDeleteRecordsRequestFuzzTest,
# maxDuration = 10s each, matching KafkaApisTest.FUZZ_DURATION)
DELETE_RECORDS_TESTS=(
    fuzzTestDeleteRecordsUnknownPartitionsOnly
    fuzzTestDeleteRecordsReplicaManagerCallback
    fuzzTestDeleteRecordsMixedKnownAndUnknownPartitions
    fuzzTestDeleteRecordsTopicAuthorizationDenied
    fuzzTestDeleteRecordsThrottledResponse
    fuzzTestDeleteRecordsForwardedInnerRequest
)

# handleInitProducerIdRequest fuzz targets (HandleInitProducerIdRequestFuzzTest,
# maxDuration = 10s each, matching KafkaApisTest.FUZZ_DURATION)
INIT_PRODUCER_ID_TESTS=(
    fuzzTestInitProducerIdTransactionalIdAuthorizationDenied
    fuzzTestInitProducerIdClusterAuthorizationDeniedNoTransactionalId
    fuzzTestInitProducerIdClusterDeniedTopicWriteByTypeAllowed
    fuzzTestInitProducerIdInvalidProducerIdOrEpochCombination
    fuzzTestInitProducerIdTxnCoordinatorSuccess
    fuzzTestInitProducerIdProducerFencedVersionRemap
    fuzzTestInitProducerIdThrottledResponse
    fuzzTestInitProducerIdForwardedInnerRequest
)

# handleCreateTopicsRequest fuzz targets (HandleCreateTopicsRequestFuzzTest,
# maxDuration = 10s each, matching KafkaApisTest.FUZZ_DURATION)
CREATE_TOPICS_TESTS=(
    fuzzTestCreateTopicsNotController
    fuzzTestCreateTopicsClusterMetadataTopicRejected
    fuzzTestCreateTopicsDuplicateNamesInRequest
    fuzzTestCreateTopicsTopicCreateAuthorizationWithoutCluster
    fuzzTestCreateTopicsDescribeConfigsAuthorization
    fuzzTestCreateTopicsAdminManagerSuccess
    fuzzTestCreateTopicsAdminManagerErrorMerge
    fuzzTestCreateTopicsAllTopicsUnauthorizedEmptyToCreate
    fuzzTestCreateTopicsThrottling
    fuzzTestCreateTopicsForwardedInnerRequest
)

# handleCreatePartitionsRequest fuzz targets (HandleCreatePartitionsRequestFuzzTest,
# maxDuration = 10s each, matching KafkaApisTest.FUZZ_DURATION)
CREATE_PARTITIONS_TESTS=(
    fuzzTestCreatePartitionsRaftAlwaysForwardUnsupported
    fuzzTestCreatePartitionsNotController
    fuzzTestCreatePartitionsDuplicateTopicNamesInRequest
    fuzzTestCreatePartitionsTopicAuthorizationDenied
    fuzzTestCreatePartitionsTopicQueuedForDeletion
    fuzzTestCreatePartitionsAdminManagerSuccess
    fuzzTestCreatePartitionsAdminManagerErrorMerge
    fuzzTestCreatePartitionsAllTopicsUnauthorizedEmptyValid
    fuzzTestCreatePartitionsEmptyTopicsList
    fuzzTestCreatePartitionsMixedDupAuthQueueAndAdmin
    fuzzTestCreatePartitionsThrottling
    fuzzTestCreatePartitionsForwardedInnerRequest
)

# handleCreateTokenRequest / handleCreateTokenRequestZk fuzz targets (HandleCreateTokenRequestFuzzTest,
# maxDuration = 10s each, matching KafkaApisTest.FUZZ_DURATION)
CREATE_DELEGATION_TOKEN_TESTS=(
    fuzzTestCreateTokenRequestNotAllowedPlaintext
    fuzzTestCreateTokenRequestAuthorizationFailedDifferentOwner
    fuzzTestCreateTokenRequestInvalidRenewerPrincipalType
    fuzzTestCreateTokenRequestZkDelegationTokenManagerSuccess
    fuzzTestCreateTokenRequestZkDelegationTokenManagerReturnsError
    fuzzTestCreateTokenRequestZkMigrationInactiveController
    fuzzTestCreateTokenRequestThrottledResponse
    fuzzTestCreateTokenRequestForwardedInnerRequest
)

# handleRenewTokenRequest / handleRenewTokenRequestZk fuzz targets (HandleRenewTokenRequestFuzzTest,
# maxDuration = 10s each, matching KafkaApisTest.FUZZ_DURATION)
RENEW_DELEGATION_TOKEN_TESTS=(
    fuzzTestRenewTokenRequestNotAllowedPlaintext
    fuzzTestRenewTokenRequestZkDelegationTokenManagerSuccess
    fuzzTestRenewTokenRequestZkDelegationTokenManagerReturnsError
    fuzzTestRenewTokenRequestZkMigrationInactiveController
    fuzzTestRenewTokenRequestThrottledResponse
    fuzzTestRenewTokenRequestForwardedInnerRequest
)

# handleExpireTokenRequest / handleExpireTokenRequestZk fuzz targets (HandleExpireTokenRequestFuzzTest,
# maxDuration = 10s each, matching KafkaApisTest.FUZZ_DURATION)
EXPIRE_DELEGATION_TOKEN_TESTS=(
    fuzzTestExpireTokenRequestNotAllowedPlaintext
    fuzzTestExpireTokenRequestZkDelegationTokenManagerSuccess
    fuzzTestExpireTokenRequestZkDelegationTokenManagerReturnsError
    fuzzTestExpireTokenRequestZkMigrationInactiveController
    fuzzTestExpireTokenRequestThrottledResponse
    fuzzTestExpireTokenRequestForwardedInnerRequest
)

# handleDescribeTokensRequest fuzz targets (HandleDescribeTokensRequestFuzzTest,
# maxDuration = 10s each, matching KafkaApisTest.FUZZ_DURATION)
DESCRIBE_DELEGATION_TOKEN_TESTS=(
    fuzzTestDescribeTokensRequestNotAllowedPlaintext
    fuzzTestDescribeTokensRequestAuthDisabled
    fuzzTestDescribeTokensRequestOwnersListExplicitlyEmpty
    fuzzTestDescribeTokensRequestOwnersNullGetTokens
    fuzzTestDescribeTokensRequestWithOwnersReturnsTokens
    fuzzTestDescribeTokensRequestThrottledResponse
    fuzzTestDescribeTokensRequestForwardedInnerRequest
)

# handleCreateAcls / AclApis.handleCreateAcls fuzz targets (HandleCreateAclsRequestFuzzTest,
# maxDuration = 10s each, matching KafkaApisTest.FUZZ_DURATION)
CREATE_ACLS_TESTS=(
    fuzzTestCreateAclsRaftAlwaysForwardUnsupported
    fuzzTestCreateAclsSecurityDisabledNoAuthorizer
    fuzzTestCreateAclsClusterAlterDenied
    fuzzTestCreateAclsInvalidClusterResourceName
    fuzzTestCreateAclsInvalidEmptyResourceName
    fuzzTestCreateAclsMixedInvalidClusterAndValidTopic
    fuzzTestCreateAclsAuthorizerCreateSuccess
    fuzzTestCreateAclsAuthorizerCreateReturnsError
    fuzzTestCreateAclsTwoCreationsBothSuccess
    fuzzTestCreateAclsThrottledResponse
    fuzzTestCreateAclsForwardedInnerRequest
    fuzzTestCreateAclsLiteralClusterNameSuccess
    fuzzTestCreateAclsRequestValidateUnknownElementsThrows
    fuzzTestCreateAclsRequestValidateV0NonLiteralPatternThrows
)

# handleDeleteAcls / AclApis.handleDeleteAcls fuzz targets (HandleDeleteAclsRequestFuzzTest,
# maxDuration = 10s each, matching KafkaApisTest.FUZZ_DURATION)
DELETE_ACLS_TESTS=(
    fuzzTestDeleteAclsRaftAlwaysForwardUnsupported
    fuzzTestDeleteAclsSecurityDisabledNoAuthorizer
    fuzzTestDeleteAclsClusterAlterDenied
    fuzzTestDeleteAclsAuthorizerDeleteEmptyResults
    fuzzTestDeleteAclsAuthorizerDeleteOneBindingSuccess
    fuzzTestDeleteAclsAuthorizerBindingDeleteError
    fuzzTestDeleteAclsAuthorizerDeleteFilterError
    fuzzTestDeleteAclsTwoFiltersBothSuccess
    fuzzTestDeleteAclsThrottledResponse
    fuzzTestDeleteAclsForwardedInnerRequest
    fuzzTestDeleteAclsWireVersion0PatternAnyNormalized
    fuzzTestDeleteAclsRequestValidateUnknownElementsThrows
    fuzzTestDeleteAclsRequestValidateV0UnsupportedPatternThrows
    fuzzTestDeleteAclsResponseV0NonLiteralMatchingAclThrows
    fuzzTestDeleteAclsResponseUnknownMatchingAclThrows
)

# handleAlterConfigsRequest fuzz targets (HandleAlterConfigsRequestFuzzTest,
# maxDuration = 10s each, matching KafkaApisTest.FUZZ_DURATION)
ALTER_CONFIGS_TESTS=(
    fuzzTestAlterConfigsZkEmptyResources
    fuzzTestAlterConfigsKRaftEmptyResources
    fuzzTestAlterConfigsKRaftPreprocessNullValueResponse
    fuzzTestAlterConfigsRaftForwardedProcessLegacyThrows
    fuzzTestAlterConfigsZkTopicAuthorizedSuccess
    fuzzTestAlterConfigsZkTopicMixedAuthorization
    fuzzTestAlterConfigsZkBrokerIdMatchSuccess
    fuzzTestAlterConfigsZkBrokerClusterWideEmptyNameSuccess
    fuzzTestAlterConfigsZkClientMetricsAuthorized
    fuzzTestAlterConfigsZkBrokerWrongIdPreprocessResponse
    fuzzTestAlterConfigsDataDuplicateResourcesPreprocess
    fuzzTestAlterConfigsDataUnrecognizedWireResourceTypePreprocess
    fuzzTestAlterConfigsDataConfigResourceTypeUnknownPreprocess
    fuzzTestAlterConfigsDataConfigResourceTypeBrokerLoggerPreprocess
    fuzzTestAlterConfigsDataDuplicateConfigKeysPreprocess
    fuzzTestAlterConfigsZkForwardingToController
    fuzzTestAlterConfigsKRaftForwardingToController
    fuzzTestAlterConfigsThrottledResponse
    fuzzTestAlterConfigsZkForwardedInnerEnvelope
    fuzzTestAlterConfigsZkAdminManagerReturnsError
)

# handleDescribeConfigsRequest fuzz targets (HandleDescribeConfigsRequestFuzzTest,
# maxDuration = 10s each, matching KafkaApisTest.FUZZ_DURATION)
DESCRIBE_CONFIGS_TESTS=(
    fuzzTestDescribeConfigsEmptyResources
    fuzzTestDescribeConfigsTopicAuthorizedKnown
    fuzzTestDescribeConfigsTopicAuthorizedUnknown
    fuzzTestDescribeConfigsTopicAuthorizationDenied
    fuzzTestDescribeConfigsTopicInvalidNameHandled
    fuzzTestDescribeConfigsClusterDeniedForBroker
    fuzzTestDescribeConfigsBrokerClusterWideAndPerBroker
    fuzzTestDescribeConfigsBrokerWrongId
    fuzzTestDescribeConfigsBrokerNonIntegerId
    fuzzTestDescribeConfigsBrokerLoggerEmptyName
    fuzzTestDescribeConfigsBrokerLoggerWrongBrokerId
    fuzzTestDescribeConfigsBrokerLoggerMatchingBrokerId
    fuzzTestDescribeConfigsClientMetricsEmptyName
    fuzzTestDescribeConfigsClientMetricsWithRepository
    fuzzTestDescribeConfigsMixedTopicDeniedAndAllowed
    fuzzTestDescribeConfigsThrottledResponse
    fuzzTestDescribeConfigsForwardedInnerRequest
    fuzzTestDescribeConfigsUnknownResourceTypeThrows
    fuzzTestDescribeConfigsNoAuthorizerSecurityDisabled
)

# handleDescribeLogDirsRequest fuzz targets (HandleDescribeLogDirsRequestFuzzTest,
# maxDuration = 10s each, matching KafkaApisTest.FUZZ_DURATION)
DESCRIBE_LOG_DIRS_TESTS=(
    fuzzTestDescribeLogDirsClusterDescribeDenied
    fuzzTestDescribeLogDirsAllTopicPartitions
    fuzzTestDescribeLogDirsSpecificSingleTopicMultiplePartitions
    fuzzTestDescribeLogDirsSpecificMultiTopic
    fuzzTestDescribeLogDirsSpecificTopicEmptyPartitionList
    fuzzTestDescribeLogDirsNoAuthorizerAllPartitions
    fuzzTestDescribeLogDirsThrottledResponse
    fuzzTestDescribeLogDirsForwardedInnerRequest
    fuzzTestDescribeLogDirsAllPartitionsNoLogs
)

# handleSaslHandshakeRequest fuzz targets (HandleSaslHandshakeRequestFuzzTest,
# maxDuration = 10s each, matching KafkaApisTest.FUZZ_DURATION)
SASL_HANDSHAKE_TESTS=(
    fuzzTestSaslHandshakeIllegalStateRandomVersionAndMechanism
    fuzzTestSaslHandshakeIllegalStateEmptyMechanism
    fuzzTestSaslHandshakeVerifiedIllegalStateResponse
    fuzzTestSaslHandshakeThrottledResponse
    fuzzTestSaslHandshakeForwardedInnerRequest
)

# handleSaslAuthenticateRequest fuzz targets (HandleSaslAuthenticateRequestFuzzTest,
# maxDuration = 10s each, matching KafkaApisTest.FUZZ_DURATION)
SASL_AUTHENTICATE_TESTS=(
    fuzzTestSaslAuthenticateIllegalStateRandomVersionAndAuthBytes
    fuzzTestSaslAuthenticateIllegalStateEmptyAuthBytes
    fuzzTestSaslAuthenticateVerifiedIllegalStateResponse
    fuzzTestSaslAuthenticateThrottledResponse
    fuzzTestSaslAuthenticateForwardedInnerRequest
)

# handleAlterReplicaLogDirsRequest fuzz targets (HandleAlterReplicaLogDirsRequestFuzzTest,
# maxDuration = 10s each, matching KafkaApisTest.FUZZ_DURATION)
ALTER_REPLICA_LOG_DIRS_TESTS=(
    fuzzTestAlterReplicaLogDirsClusterAuthorizationDenied
    fuzzTestAlterReplicaLogDirsEmptyDirsAuthorized
    fuzzTestAlterReplicaLogDirsSingleTopicMultiplePartitions
    fuzzTestAlterReplicaLogDirsMultipleTopicsGrouped
    fuzzTestAlterReplicaLogDirsTwoDirectories
    fuzzTestAlterReplicaLogDirsNoAuthorizer
    fuzzTestAlterReplicaLogDirsThrottledResponse
    fuzzTestAlterReplicaLogDirsForwardedInnerRequest
    fuzzTestAlterReplicaLogDirsDuplicateTopicPartitionLastDirWins
)

# handleDeleteTopicsRequest fuzz targets (HandleDeleteTopicsRequestFuzzTest,
# maxDuration = 10s each, matching KafkaApisTest.FUZZ_DURATION)
DELETE_TOPICS_TESTS=(
    fuzzTestDeleteTopicsNotController
    fuzzTestDeleteTopicsDeletionDisabled
    fuzzTestDeleteTopicsInvalidNameAndNonZeroTopicId
    fuzzTestDeleteTopicsUnknownTopicByName
    fuzzTestDeleteTopicsByIdDescribeDenied
    fuzzTestDeleteTopicsByNameDeleteDenied
    fuzzTestDeleteTopicsByIdUnknownTopicId
    fuzzTestDeleteTopicsByIdDeleteDenied
    fuzzTestDeleteTopicsAdminManagerSuccess
    fuzzTestDeleteTopicsAdminManagerCallbackErrors
    fuzzTestDeleteTopicsEmptyToDeleteImmediateResponse
    fuzzTestDeleteTopicsThrottledResponse
    fuzzTestDeleteTopicsForwardedInnerRequest
)

# handleOffsetForLeaderEpochRequest fuzz targets (HandleOffsetForLeaderEpochRequestFuzzTest,
# maxDuration = 10s each, matching KafkaApisTest.FUZZ_DURATION)
OFFSET_FOR_LEADER_EPOCH_TESTS=(
    fuzzTestOffsetForLeaderEpochNoAuthorizerClusterPath
    fuzzTestOffsetForLeaderEpochAuthorizerClusterActionAllowed
    fuzzTestOffsetForLeaderEpochClusterDeniedMixedDescribe
    fuzzTestOffsetForLeaderEpochClusterDeniedAllTopicsUnauthorized
    fuzzTestOffsetForLeaderEpochEmptyTopics
    fuzzTestOffsetForLeaderEpochMultiPartitionAuthorized
    fuzzTestOffsetForLeaderEpochConsumerBuilderPath
    fuzzTestOffsetForLeaderEpochThrottledResponse
    fuzzTestOffsetForLeaderEpochForwardedInnerRequest
)

# handleAddPartitionsToTxnRequest fuzz targets (HandleAddPartitionsToTxnRequestFuzzTest,
# maxDuration = 10s each, matching KafkaApisTest.FUZZ_DURATION)
ADD_PARTITIONS_TO_TXN_TESTS=(
    fuzzTestAddPartitionsToTxnClientSuccess
    fuzzTestAddPartitionsToTxnClientTransactionalIdDenied
    fuzzTestAddPartitionsToTxnClientTopicWriteDenied
    fuzzTestAddPartitionsToTxnClientUnknownPartitionMixed
    fuzzTestAddPartitionsToTxnClientProducerFencedRemappedLegacy
    fuzzTestAddPartitionsToTxnClientProducerFencedModern
    fuzzTestAddPartitionsToTxnBrokerAddPartitions
    fuzzTestAddPartitionsToTxnBrokerVerifyOnly
    fuzzTestAddPartitionsToTxnBrokerBatchedTwoTransactions
    fuzzTestAddPartitionsToTxnBrokerClusterActionDenied
    fuzzTestAddPartitionsToTxnBrokerNullTransactionalId
    fuzzTestAddPartitionsToTxnThrottledResponse
    fuzzTestAddPartitionsToTxnForwardedInnerRequest
    fuzzTestAddPartitionsToTxnUnsupportedInterBrokerVersion
)

# handleAddOffsetsToTxnRequest fuzz targets (HandleAddOffsetsToTxnRequestFuzzTest,
# maxDuration = 10s each, matching KafkaApisTest.FUZZ_DURATION)
ADD_OFFSETS_TO_TXN_TESTS=(
    fuzzTestAddOffsetsToTxnSuccess
    fuzzTestAddOffsetsToTxnTransactionalIdWriteDenied
    fuzzTestAddOffsetsToTxnGroupReadDenied
    fuzzTestAddOffsetsToTxnProducerFencedLegacyClient
    fuzzTestAddOffsetsToTxnProducerFencedModernClient
    fuzzTestAddOffsetsToTxnCoordinatorConcurrentTransactions
    fuzzTestAddOffsetsToTxnThrottledResponse
    fuzzTestAddOffsetsToTxnForwardedInnerRequest
    fuzzTestAddOffsetsToTxnUnsupportedInterBrokerVersion
)

# handleEndTxnRequest fuzz targets (HandleEndTxnRequestFuzzTest,
# maxDuration = 10s each, matching KafkaApisTest.FUZZ_DURATION)
END_TXN_TESTS=(
    fuzzTestEndTxnSuccessCommit
    fuzzTestEndTxnSuccessAbort
    fuzzTestEndTxnTransactionalIdWriteDenied
    fuzzTestEndTxnProducerFencedLegacyClient
    fuzzTestEndTxnProducerFencedModernClient
    fuzzTestEndTxnCoordinatorConcurrentTransactions
    fuzzTestEndTxnThrottledResponse
    fuzzTestEndTxnForwardedInnerRequest
    fuzzTestEndTxnUnsupportedInterBrokerVersion
)

# handleWriteTxnMarkersRequest fuzz targets (HandleWriteTxnMarkersRequestFuzzTest,
# maxDuration = 10s each, matching KafkaApisTest.FUZZ_DURATION)
WRITE_TXN_MARKERS_TESTS=(
    fuzzTestWriteTxnMarkersEmptyMarkers
    fuzzTestWriteTxnMarkersClusterAuthorizationDenied
    fuzzTestWriteTxnMarkersUnknownTopicOrPartition
    fuzzTestWriteTxnMarkersUnsupportedMessageFormat
    fuzzTestWriteTxnMarkersAppendSuccessClassicCoordinator
    fuzzTestWriteTxnMarkersNewGroupCoordinatorOffsetsTopic
    fuzzTestWriteTxnMarkersMixedMagicAppendAndError
    fuzzTestWriteTxnMarkersForwardedInnerRequest
    fuzzTestWriteTxnMarkersUnsupportedInterBrokerVersion
)

# handleOffsetCommitRequest fuzz targets (HandleOffsetCommitRequestFuzzTest,
# maxDuration = 10s each, matching KafkaApisTest.FUZZ_DURATION)
OFFSET_COMMIT_TESTS=(
    fuzzTestOffsetCommitGroupReadDenied
    fuzzTestOffsetCommitStaticMembershipUnsupported
    fuzzTestOffsetCommitTopicReadDenied
    fuzzTestOffsetCommitUnknownTopic
    fuzzTestOffsetCommitUnknownPartition
    fuzzTestOffsetCommitNoAuthorizedPartitions
    fuzzTestOffsetCommitCoordinatorSuccess
    fuzzTestOffsetCommitCoordinatorCompleteExceptionally
    fuzzTestOffsetCommitCommitOffsetsThrowsSync
    fuzzTestOffsetCommitThrottledResponse
    fuzzTestOffsetCommitForwardedInnerRequest
    fuzzTestOffsetCommitVersion0ZkPaths
    fuzzTestOffsetCommitVersion0RaftUnsupported
)

# handleTxnOffsetCommitRequest fuzz targets (HandleTxnOffsetCommitRequestFuzzTest,
# maxDuration = 10s each, matching KafkaApisTest.FUZZ_DURATION)
TXN_OFFSET_COMMIT_TESTS=(
    fuzzTestTxnOffsetCommitTransactionalIdWriteDenied
    fuzzTestTxnOffsetCommitGroupReadDenied
    fuzzTestTxnOffsetCommitTopicReadDeniedMixedTopics
    fuzzTestTxnOffsetCommitUnknownTopic
    fuzzTestTxnOffsetCommitUnknownPartitionAndValidSameTopic
    fuzzTestTxnOffsetCommitOnlyInvalidPartitionsNoCoordinator
    fuzzTestTxnOffsetCommitUnknownTopicAndValidTopic
    fuzzTestTxnOffsetCommitCoordinatorSuccess
    fuzzTestTxnOffsetCommitCoordinatorCompleteExceptionally
    fuzzTestTxnOffsetCommitThrottleResponse
    fuzzTestTxnOffsetCommitForwardedInnerRequest
    fuzzTestTxnOffsetCommitUnsupportedInterBrokerVersion
    fuzzTestTxnOffsetCommitV3GroupMetadataPassthrough
    fuzzTestTxnOffsetCommitCoordinatorLoadInProgressLegacyRemap
)

# handleLeaderAndIsrRequest fuzz targets (HandleLeaderAndIsrRequestFuzzTest,
# maxDuration = 10s each, matching KafkaApisTest.FUZZ_DURATION)
LEADER_AND_ISR_TESTS=(
    fuzzTestLeaderAndIsrBecomeLeaderOrFollowerSuccess
    fuzzTestLeaderAndIsrStaleBrokerEpoch
    fuzzTestLeaderAndIsrUnknownBrokerEpoch
    fuzzTestLeaderAndIsrClusterActionDenied
    fuzzTestLeaderAndIsrRaftShouldNeverReceive
    fuzzTestLeaderAndIsrKRaftControllerMissingLifecycleManager
    fuzzTestLeaderAndIsrKRaftControllerUnknownBrokerEpoch
    fuzzTestLeaderAndIsrForwardedInnerRequest
)

# handleTopicMetadataRequest fuzz targets (HandleTopicMetadataRequestFuzzTest,
# maxDuration = 10s each, matching KafkaApisTest.FUZZ_DURATION)
TOPIC_METADATA_TESTS=(
    fuzzTestTopicMetadataInvalidNullTopicName
    fuzzTestTopicMetadataInvalidTopicIdPreV12
    fuzzTestTopicMetadataAllTopics
    fuzzTestTopicMetadataByNameKnownTopic
    fuzzTestTopicMetadataUnknownTopicIdV12
    fuzzTestTopicMetadataMixedTopicIdsAuthorized
    fuzzTestTopicMetadataDescribeDeniedByName
    fuzzTestTopicMetadataThrottledResponse
    fuzzTestTopicMetadataForwardedInnerRequest
    fuzzTestTopicMetadataIncludeAuthorizedOperations
    fuzzTestTopicMetadataVersionZeroEmptyMeansAllTopics
    fuzzTestTopicMetadataAutoCreateNonExistingTopic
)

# handleGetTelemetrySubscriptionsRequest fuzz targets (HandleGetTelemetrySubscriptionsRequestFuzzTest,
# maxDuration = 10s each, matching KafkaApisTest.FUZZ_DURATION)
GET_TELEMETRY_SUBSCRIPTIONS_TESTS=(
    fuzzTestGetTelemetrySubscriptionsZkUnsupportedVersion
    fuzzTestGetTelemetrySubscriptionsZkUnsupportedThrottled
    fuzzTestGetTelemetrySubscriptionsZkUnsupportedForwarded
    fuzzTestGetTelemetrySubscriptionsKRaftSuccess
    fuzzTestGetTelemetrySubscriptionsKRaftProcessThrowsMapsToInvalidRequest
    fuzzTestGetTelemetrySubscriptionsKRaftProcessThrowsInvalidRequestException
    fuzzTestGetTelemetrySubscriptionsKRaftManagerReturnsErrorCode
    fuzzTestGetTelemetrySubscriptionsKRaftForwardedThrottled
)

# handlePushTelemetryRequest fuzz targets (HandlePushTelemetryRequestFuzzTest,
# maxDuration = 10s each, matching KafkaApisTest.FUZZ_DURATION)
PUSH_TELEMETRY_TESTS=(
    fuzzTestPushTelemetryZkUnsupportedVersion
    fuzzTestPushTelemetryZkUnsupportedThrottled
    fuzzTestPushTelemetryZkUnsupportedForwarded
    fuzzTestPushTelemetryKRaftSuccess
    fuzzTestPushTelemetryKRaftProcessThrowsMapsToInvalidRequest
    fuzzTestPushTelemetryKRaftProcessThrowsInvalidRequestException
    fuzzTestPushTelemetryKRaftManagerReturnsErrorCode
    fuzzTestPushTelemetryKRaftForwardedThrottled
)

mkdir -p "$JACOCO_DIR"

run_one() {
    local fqcn="$1"
    local t="$2"
    echo "=========================================="
    echo "[fuzz] running ${fqcn}.${t}"
    echo "=========================================="
    set +e
    ./gradlew :core:test --no-daemon --rerun-tasks \
        --tests "${fqcn}.${t}" \
        -i 2>&1 | tee "$JACOCO_DIR/run_${t}.log" | \
        grep -E "(Fuzzing|fuzzTest.*PASSED|fuzzTest.*FAILED|Done [0-9]+ runs|<empty input>|exec/s: [0-9]+ |BUILD |^FAILURE|crash-)" || true
    local st=${PIPESTATUS[0]}
    set -e
    if [[ $st -ne 0 ]]; then
        echo "[fuzz] ERROR: gradlew exited with status $st for ${fqcn}.${t}" >&2
        exit $st
    fi
}

if [[ "$FUZZ_SUITE" == "all" ]]; then
    for t in "${PRODUCE_TESTS[@]}"; do
        run_one "unit.kafka.server.fuzz.HandleProduceRequestFuzzTest" "$t"
    done
    for t in "${FETCH_TESTS[@]}"; do
        run_one "unit.kafka.server.fuzz.HandleFetchRequestFuzzTest" "$t"
    done
    for t in "${DESCRIBE_TP_TESTS[@]}"; do
        run_one "unit.kafka.server.fuzz.HandleDescribeTopicPartitionsRequestFuzzTest" "$t"
    done
    for t in "${HEARTBEAT_TESTS[@]}"; do
        run_one "unit.kafka.server.fuzz.HandleHeartbeatRequestFuzzTest" "$t"
    done
    for t in "${OFFSET_FETCH_TESTS[@]}"; do
        run_one "unit.kafka.server.fuzz.HandleOffsetFetchRequestFuzzTest" "$t"
    done
    for t in "${SYNC_GROUP_TESTS[@]}"; do
        run_one "unit.kafka.server.fuzz.HandleSyncGroupRequestFuzzTest" "$t"
    done
    for t in "${LEAVE_GROUP_TESTS[@]}"; do
        run_one "unit.kafka.server.fuzz.HandleLeaveGroupRequestFuzzTest" "$t"
    done
    for t in "${DELETE_GROUPS_TESTS[@]}"; do
        run_one "unit.kafka.server.fuzz.HandleDeleteGroupsRequestFuzzTest" "$t"
    done
    for t in "${DESCRIBE_GROUPS_TESTS[@]}"; do
        run_one "unit.kafka.server.fuzz.HandleDescribeGroupsRequestFuzzTest" "$t"
    done
    for t in "${DESCRIBE_ACLS_TESTS[@]}"; do
        run_one "unit.kafka.server.fuzz.HandleDescribeAclsRequestFuzzTest" "$t"
    done
    for t in "${LIST_GROUPS_TESTS[@]}"; do
        run_one "unit.kafka.server.fuzz.HandleListGroupsRequestFuzzTest" "$t"
    done
    for t in "${API_VERSIONS_TESTS[@]}"; do
        run_one "unit.kafka.server.fuzz.HandleApiVersionsRequestFuzzTest" "$t"
    done
    for t in "${DELETE_RECORDS_TESTS[@]}"; do
        run_one "unit.kafka.server.fuzz.HandleDeleteRecordsRequestFuzzTest" "$t"
    done
    for t in "${INIT_PRODUCER_ID_TESTS[@]}"; do
        run_one "unit.kafka.server.fuzz.HandleInitProducerIdRequestFuzzTest" "$t"
    done
    for t in "${CREATE_TOPICS_TESTS[@]}"; do
        run_one "unit.kafka.server.fuzz.HandleCreateTopicsRequestFuzzTest" "$t"
    done
    for t in "${CREATE_PARTITIONS_TESTS[@]}"; do
        run_one "unit.kafka.server.fuzz.HandleCreatePartitionsRequestFuzzTest" "$t"
    done
    for t in "${CREATE_DELEGATION_TOKEN_TESTS[@]}"; do
        run_one "unit.kafka.server.fuzz.HandleCreateTokenRequestFuzzTest" "$t"
    done
    for t in "${RENEW_DELEGATION_TOKEN_TESTS[@]}"; do
        run_one "unit.kafka.server.fuzz.HandleRenewTokenRequestFuzzTest" "$t"
    done
    for t in "${EXPIRE_DELEGATION_TOKEN_TESTS[@]}"; do
        run_one "unit.kafka.server.fuzz.HandleExpireTokenRequestFuzzTest" "$t"
    done
    for t in "${DESCRIBE_DELEGATION_TOKEN_TESTS[@]}"; do
        run_one "unit.kafka.server.fuzz.HandleDescribeTokensRequestFuzzTest" "$t"
    done
    for t in "${CREATE_ACLS_TESTS[@]}"; do
        run_one "unit.kafka.server.fuzz.HandleCreateAclsRequestFuzzTest" "$t"
    done
    for t in "${DELETE_ACLS_TESTS[@]}"; do
        run_one "unit.kafka.server.fuzz.HandleDeleteAclsRequestFuzzTest" "$t"
    done
    for t in "${ALTER_CONFIGS_TESTS[@]}"; do
        run_one "unit.kafka.server.fuzz.HandleAlterConfigsRequestFuzzTest" "$t"
    done
    for t in "${DESCRIBE_CONFIGS_TESTS[@]}"; do
        run_one "unit.kafka.server.fuzz.HandleDescribeConfigsRequestFuzzTest" "$t"
    done
    for t in "${DESCRIBE_LOG_DIRS_TESTS[@]}"; do
        run_one "unit.kafka.server.fuzz.HandleDescribeLogDirsRequestFuzzTest" "$t"
    done
    for t in "${SASL_AUTHENTICATE_TESTS[@]}"; do
        run_one "unit.kafka.server.fuzz.HandleSaslAuthenticateRequestFuzzTest" "$t"
    done
    for t in "${SASL_HANDSHAKE_TESTS[@]}"; do
        run_one "unit.kafka.server.fuzz.HandleSaslHandshakeRequestFuzzTest" "$t"
    done
    for t in "${ALTER_REPLICA_LOG_DIRS_TESTS[@]}"; do
        run_one "unit.kafka.server.fuzz.HandleAlterReplicaLogDirsRequestFuzzTest" "$t"
    done
    for t in "${DELETE_TOPICS_TESTS[@]}"; do
        run_one "unit.kafka.server.fuzz.HandleDeleteTopicsRequestFuzzTest" "$t"
    done
    for t in "${OFFSET_FOR_LEADER_EPOCH_TESTS[@]}"; do
        run_one "unit.kafka.server.fuzz.HandleOffsetForLeaderEpochRequestFuzzTest" "$t"
    done
    for t in "${ADD_PARTITIONS_TO_TXN_TESTS[@]}"; do
        run_one "unit.kafka.server.fuzz.HandleAddPartitionsToTxnRequestFuzzTest" "$t"
    done
    for t in "${ADD_OFFSETS_TO_TXN_TESTS[@]}"; do
        run_one "unit.kafka.server.fuzz.HandleAddOffsetsToTxnRequestFuzzTest" "$t"
    done
    for t in "${END_TXN_TESTS[@]}"; do
        run_one "unit.kafka.server.fuzz.HandleEndTxnRequestFuzzTest" "$t"
    done
    for t in "${WRITE_TXN_MARKERS_TESTS[@]}"; do
        run_one "unit.kafka.server.fuzz.HandleWriteTxnMarkersRequestFuzzTest" "$t"
    done
    for t in "${OFFSET_COMMIT_TESTS[@]}"; do
        run_one "unit.kafka.server.fuzz.HandleOffsetCommitRequestFuzzTest" "$t"
    done
    for t in "${TXN_OFFSET_COMMIT_TESTS[@]}"; do
        run_one "unit.kafka.server.fuzz.HandleTxnOffsetCommitRequestFuzzTest" "$t"
    done
    for t in "${LEADER_AND_ISR_TESTS[@]}"; do
        run_one "unit.kafka.server.fuzz.HandleLeaderAndIsrRequestFuzzTest" "$t"
    done
    for t in "${TOPIC_METADATA_TESTS[@]}"; do
        run_one "unit.kafka.server.fuzz.HandleTopicMetadataRequestFuzzTest" "$t"
    done
    for t in "${GET_TELEMETRY_SUBSCRIPTIONS_TESTS[@]}"; do
        run_one "unit.kafka.server.fuzz.HandleGetTelemetrySubscriptionsRequestFuzzTest" "$t"
    done
    for t in "${PUSH_TELEMETRY_TESTS[@]}"; do
        run_one "unit.kafka.server.fuzz.HandlePushTelemetryRequestFuzzTest" "$t"
    done
elif [[ "$FUZZ_SUITE" == "offset-fetch" ]]; then
    for t in "${OFFSET_FETCH_TESTS[@]}"; do
        run_one "unit.kafka.server.fuzz.HandleOffsetFetchRequestFuzzTest" "$t"
    done
elif [[ "$FUZZ_SUITE" == "describe-configs" ]]; then
    for t in "${DESCRIBE_CONFIGS_TESTS[@]}"; do
        run_one "unit.kafka.server.fuzz.HandleDescribeConfigsRequestFuzzTest" "$t"
    done
elif [[ "$FUZZ_SUITE" == "describe-log-dirs" ]]; then
    for t in "${DESCRIBE_LOG_DIRS_TESTS[@]}"; do
        run_one "unit.kafka.server.fuzz.HandleDescribeLogDirsRequestFuzzTest" "$t"
    done
elif [[ "$FUZZ_SUITE" == "sasl-authenticate" ]]; then
    for t in "${SASL_AUTHENTICATE_TESTS[@]}"; do
        run_one "unit.kafka.server.fuzz.HandleSaslAuthenticateRequestFuzzTest" "$t"
    done
elif [[ "$FUZZ_SUITE" == "sasl-handshake" ]]; then
    for t in "${SASL_HANDSHAKE_TESTS[@]}"; do
        run_one "unit.kafka.server.fuzz.HandleSaslHandshakeRequestFuzzTest" "$t"
    done
elif [[ "$FUZZ_SUITE" == "alter-replica-log-dirs" ]]; then
    for t in "${ALTER_REPLICA_LOG_DIRS_TESTS[@]}"; do
        run_one "unit.kafka.server.fuzz.HandleAlterReplicaLogDirsRequestFuzzTest" "$t"
    done
elif [[ "$FUZZ_SUITE" == "create-partitions" ]]; then
    for t in "${CREATE_PARTITIONS_TESTS[@]}"; do
        run_one "unit.kafka.server.fuzz.HandleCreatePartitionsRequestFuzzTest" "$t"
    done
elif [[ "$FUZZ_SUITE" == "create-delegation-token" ]]; then
    for t in "${CREATE_DELEGATION_TOKEN_TESTS[@]}"; do
        run_one "unit.kafka.server.fuzz.HandleCreateTokenRequestFuzzTest" "$t"
    done
elif [[ "$FUZZ_SUITE" == "renew-delegation-token" ]]; then
    for t in "${RENEW_DELEGATION_TOKEN_TESTS[@]}"; do
        run_one "unit.kafka.server.fuzz.HandleRenewTokenRequestFuzzTest" "$t"
    done
elif [[ "$FUZZ_SUITE" == "expire-delegation-token" ]]; then
    for t in "${EXPIRE_DELEGATION_TOKEN_TESTS[@]}"; do
        run_one "unit.kafka.server.fuzz.HandleExpireTokenRequestFuzzTest" "$t"
    done
elif [[ "$FUZZ_SUITE" == "describe-delegation-token" ]]; then
    for t in "${DESCRIBE_DELEGATION_TOKEN_TESTS[@]}"; do
        run_one "unit.kafka.server.fuzz.HandleDescribeTokensRequestFuzzTest" "$t"
    done
elif [[ "$FUZZ_SUITE" == "delete-groups" ]]; then
    for t in "${DELETE_GROUPS_TESTS[@]}"; do
        run_one "unit.kafka.server.fuzz.HandleDeleteGroupsRequestFuzzTest" "$t"
    done
elif [[ "$FUZZ_SUITE" == "get-telemetry-subscriptions" ]]; then
    for t in "${GET_TELEMETRY_SUBSCRIPTIONS_TESTS[@]}"; do
        run_one "unit.kafka.server.fuzz.HandleGetTelemetrySubscriptionsRequestFuzzTest" "$t"
    done
elif [[ "$FUZZ_SUITE" == "push-telemetry" ]]; then
    for t in "${PUSH_TELEMETRY_TESTS[@]}"; do
        run_one "unit.kafka.server.fuzz.HandlePushTelemetryRequestFuzzTest" "$t"
    done
fi


# 4. Generate HTML/XML/CSV reports against the freshly compiled core classes.
mkdir -p "$REPORT_DIR"
java -jar "$CLI" report "$EXEC" \
    --classfiles "$REPO_ROOT/core/build/classes/scala/main/kafka/server" \
    --sourcefiles "$REPO_ROOT/core/src/main/scala" \
    --html "$REPORT_DIR/html" \
    --xml  "$REPORT_DIR/coverage.xml" \
    --csv  "$REPORT_DIR/coverage.csv" \
    --name "KafkaApis fuzz coverage (Handle*RequestFuzzTest)"

echo
echo "[fuzz] HTML report : $REPORT_DIR/html/index.html"
echo "[fuzz] XML  report : $REPORT_DIR/coverage.xml"
echo "[fuzz] exec data   : $EXEC"

# 4a. Refresh the trimmed in-tree report under core/src/test/fuzz/coverage_results/
#     (KafkaApis + RequestHandlerHelper only; see refresh_in_repo_coverage.py).
if [[ -f "$REPORT_DIR/coverage.xml" ]]; then
    python3 "$SCRIPT_DIR/refresh_in_repo_coverage.py" || echo "[fuzz] warning: refresh_in_repo_coverage.py failed" >&2
fi

# 4b. Optionally re-snapshot the freshly produced exec file into the
#     committed in-tree location so it survives a Cloud Agent VM
#     recycle and a future `FUZZ_MODE=resume` can pick up where this
#     run left off. xz -9e gives ~80x compression on the mostly-zero
#     coverage data (40 MB -> ~500 KB) which is fine to commit.
if [[ "$FUZZ_MODE" == "snapshot" ]]; then
    echo "[fuzz] re-snapshotting $EXEC -> $COMMITTED_EXEC_XZ"
    mkdir -p "$(dirname "$COMMITTED_EXEC_XZ")"
    xz -9e -f -c "$EXEC" > "$COMMITTED_EXEC_XZ"
    ls -la "$COMMITTED_EXEC_XZ"
fi

# 5. Print a focused summary of both fuzz targets' coverage.
python3 - "$REPORT_DIR/coverage.xml" <<'PY'
import sys, xml.etree.ElementTree as ET

TARGETS = [
    ("KafkaApis.scala",          "handleProduceRequest",                  606, 752),
    ("KafkaApis.scala",          "handleFetchRequest",                    757, 1077),
    ("KafkaApis.scala",          "handleDescribeTopicPartitionsRequest", 1445, 1461),
    ("KafkaApis.scala",          "handleGetTelemetrySubscriptionsRequest", 3933, 3948),
    ("KafkaApis.scala",          "handlePushTelemetryRequest", 3950, 3965),
    ("KafkaApis.scala",          "handleHeartbeatRequest",               1924, 1948),
    ("KafkaApis.scala",          "handleDeleteGroupsRequest",            1886, 1922),
    ("KafkaApis.scala",          "handleOffsetFetchRequest",             1466, 1630),
    ("KafkaApis.scala",          "handleSaslHandshakeRequest",           1970, 1973),
    ("KafkaApis.scala",          "handleSaslAuthenticateRequest",       1975, 1980),
    ("KafkaApis.scala",          "handleCreatePartitionsRequest",        2100, 2148),
    ("KafkaApis.scala",          "handleCreateTokenRequest",              3162, 3189),
    ("KafkaApis.scala",          "handleCreateTokenRequestZk",           3192, 3229),
    ("KafkaApis.scala",          "handleRenewTokenRequest",               3232, 3243),
    ("KafkaApis.scala",          "handleRenewTokenRequestZk",            3245, 3276),
    ("KafkaApis.scala",          "handleExpireTokenRequest",              3278, 3289),
    ("KafkaApis.scala",          "handleExpireTokenRequestZk",           3291, 3322),
    ("KafkaApis.scala",          "handleDescribeTokensRequest",          3324, 3358),
    ("KafkaApis.scala",          "handleDescribeConfigsRequest",         3111, 3115),
    ("KafkaApis.scala",          "handleAlterReplicaLogDirsRequest",     3117, 3137),
    ("KafkaApis.scala",          "handleDescribeLogDirsRequest",         3139, 3160),
    ("RequestHandlerHelper.scala", "sendMaybeThrottle",                   112,  122),
]

tree = ET.parse(sys.argv[1])
root = tree.getroot()

def find_src(filename):
    for pkg in root.findall('package'):
        if pkg.get('name') == 'kafka/server':
            for sf in pkg.findall('sourcefile'):
                if sf.get('name') == filename:
                    return sf
    return None

for filename, name, start, end in TARGETS:
    src = find_src(filename)
    if src is None:
        print("%s: source not found in report" % filename); continue
    lines_in = [l for l in src.findall('line') if start <= int(l.get('nr')) <= end]
    mi = ci = mb = cb = 0
    covered, missed = [], []
    for l in lines_in:
        mi_l = int(l.get('mi')); ci_l = int(l.get('ci'))
        mb_l = int(l.get('mb')); cb_l = int(l.get('cb'))
        mi += mi_l; ci += ci_l; mb += mb_l; cb += cb_l
        nr = int(l.get('nr'))
        if ci_l > 0:    covered.append(nr)
        elif mi_l > 0:  missed.append(nr)
    total_lines = len(covered) + len(missed)
    total_instr = mi + ci
    total_br = mb + cb
    print()
    print("%s (%s lines %d-%d):" % (name, filename, start, end))
    if total_lines:
        print("  Source lines  : %d / %d  (%.1f%%)" % (
            len(covered), total_lines, 100.0*len(covered)/total_lines))
    if total_instr:
        print("  Instructions  : %d / %d  (%.1f%%)" % (
            ci, total_instr, 100.0*ci/total_instr))
    if total_br:
        print("  Branches      : %d / %d  (%.1f%%)" % (
            cb, total_br, 100.0*cb/total_br))
    print("  Missed lines  : %s" % missed)
PY
