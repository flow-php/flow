<?php

declare(strict_types=1);

namespace Flow\Telemetry\Resource\Detector;

use Flow\Telemetry\Resource;
use Flow\Telemetry\Resource\Attribute\ServiceAttribute;
use Flow\Telemetry\Resource\ResourceDetector;

use function explode;
use function getenv;
use function strpos;
use function substr;
use function trim;
use function urldecode;

/**
 * Detects resource attributes from environment variables.
 *
 * Reads the following OpenTelemetry standard environment variables:
 * - OTEL_SERVICE_NAME: Sets service.name attribute
 * - OTEL_RESOURCE_ATTRIBUTES: Sets additional attributes in key=value,key2=value2 format
 *
 * Attributes from OTEL_RESOURCE_ATTRIBUTES follow the format specified by OpenTelemetry:
 * ```
 * OTEL_RESOURCE_ATTRIBUTES=key1=value1,key2=value2
 * ```
 *
 * Per the OpenTelemetry Resource SDK specification, the `,` and `=` characters in keys and
 * values MUST be percent-encoded (other characters MAY be percent-encoded); both keys and
 * values are percent-decoded:
 * ```
 * OTEL_RESOURCE_ATTRIBUTES=key=value%2Cwith%2Ccommas
 * ```
 *
 * OTEL_SERVICE_NAME takes precedence over service.name in OTEL_RESOURCE_ATTRIBUTES.
 *
 * Example:
 * ```bash
 * export OTEL_SERVICE_NAME=my-service
 * export OTEL_RESOURCE_ATTRIBUTES=service.version=1.0.0,deployment.environment.name=production
 * ```
 */
final readonly class EnvironmentDetector implements ResourceDetector
{
    private const string OTEL_RESOURCE_ATTRIBUTES = 'OTEL_RESOURCE_ATTRIBUTES';

    private const string OTEL_SERVICE_NAME = 'OTEL_SERVICE_NAME';

    public function detect(): Resource
    {
        $attributes = $this->parseResourceAttributes();

        $serviceName = $this->getEnv(self::OTEL_SERVICE_NAME);

        if ($serviceName !== null) {
            $attributes[ServiceAttribute::NAME->value] = $serviceName;
        }

        return Resource::create($attributes);
    }

    private function getEnv(string $name): ?string
    {
        $value = getenv($name);

        if ($value === false || $value === '') {
            return null;
        }

        return $value;
    }

    /**
     * @return array<string, string>
     */
    private function parseResourceAttributes(): array
    {
        $rawAttributes = $this->getEnv(self::OTEL_RESOURCE_ATTRIBUTES);

        if ($rawAttributes === null) {
            return [];
        }

        $attributes = [];

        foreach (explode(',', $rawAttributes) as $pair) {
            $pair = trim($pair);

            if ($pair === '') {
                continue;
            }

            $equalsPos = strpos($pair, '=');

            if ($equalsPos === false) {
                continue;
            }

            $key = urldecode(trim(substr($pair, 0, $equalsPos)));

            if ($key === '') {
                continue;
            }

            $attributes[$key] = urldecode(trim(substr($pair, $equalsPos + 1)));
        }

        return $attributes;
    }
}
