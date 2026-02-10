<?php

declare(strict_types=1);

namespace Flow\Telemetry\Resource\Attribute;

/**
 * Cloud platform values following OpenTelemetry semantic conventions.
 *
 * These values correspond to the `cloud.platform` attribute as defined in the
 * OpenTelemetry semantic conventions specification.
 *
 * @see https://opentelemetry.io/docs/specs/semconv/resource/cloud/
 */
enum CloudPlatform : string
{
    case ALIBABA_CLOUD_ECS = 'alibaba_cloud_ecs';

    case ALIBABA_CLOUD_FC = 'alibaba_cloud_fc';

    case ALIBABA_CLOUD_OPENSHIFT = 'alibaba_cloud_openshift';

    case AWS_APP_RUNNER = 'aws_app_runner';

    case AWS_EC2 = 'aws_ec2';

    case AWS_ECS = 'aws_ecs';

    case AWS_EKS = 'aws_eks';

    case AWS_ELASTIC_BEANSTALK = 'aws_elastic_beanstalk';

    case AWS_LAMBDA = 'aws_lambda';

    case AWS_OPENSHIFT = 'aws_openshift';

    case AZURE_AKS = 'azure_aks';

    case AZURE_APP_SERVICE = 'azure_app_service';

    case AZURE_CONTAINER_APPS = 'azure_container_apps';

    case AZURE_CONTAINER_INSTANCES = 'azure_container_instances';

    case AZURE_FUNCTIONS = 'azure_functions';

    case AZURE_OPENSHIFT = 'azure_openshift';

    case AZURE_VM = 'azure_vm';

    case GCP_APP_ENGINE = 'gcp_app_engine';

    case GCP_CLOUD_FUNCTIONS = 'gcp_cloud_functions';

    case GCP_CLOUD_RUN = 'gcp_cloud_run';

    case GCP_COMPUTE_ENGINE = 'gcp_compute_engine';

    case GCP_KUBERNETES_ENGINE = 'gcp_kubernetes_engine';

    case GCP_OPENSHIFT = 'gcp_openshift';

    case IBM_CLOUD_CODE_ENGINE = 'ibm_cloud_code_engine';

    case IBM_CLOUD_OPENSHIFT = 'ibm_cloud_openshift';

    case TENCENT_CLOUD_CVM = 'tencent_cloud_cvm';

    case TENCENT_CLOUD_EKS = 'tencent_cloud_eks';

    case TENCENT_CLOUD_SCF = 'tencent_cloud_scf';
}
