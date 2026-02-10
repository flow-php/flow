<?php

declare(strict_types=1);

namespace Flow\Telemetry\Resource\Attribute;

/**
 * Cloud provider values following OpenTelemetry semantic conventions.
 *
 * These values correspond to the `cloud.provider` attribute as defined in the
 * OpenTelemetry semantic conventions specification.
 *
 * @see https://opentelemetry.io/docs/specs/semconv/resource/cloud/
 */
enum CloudProvider : string
{
    case ALIBABA_CLOUD = 'alibaba_cloud';

    case AWS = 'aws';

    case AZURE = 'azure';

    case GCP = 'gcp';

    case HEROKU = 'heroku';

    case IBM_CLOUD = 'ibm_cloud';

    case TENCENT_CLOUD = 'tencent_cloud';
}
