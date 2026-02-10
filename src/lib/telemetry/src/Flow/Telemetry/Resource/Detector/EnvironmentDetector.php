<?php

declare(strict_types=1);

namespace Flow\Telemetry\Resource\Detector;

use Flow\Telemetry\Resource;
use Flow\Telemetry\Resource\Attribute\ServiceAttribute;
use Flow\Telemetry\Resource\ResourceDetector;

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
 * Special characters in values can be escaped with backslash:
 * ```
 * OTEL_RESOURCE_ATTRIBUTES=key=value\,with\,commas
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

    public function detect() : Resource
    {
        $attributes = $this->parseResourceAttributes();

        $serviceName = $this->getEnv(self::OTEL_SERVICE_NAME);

        if ($serviceName !== null) {
            $attributes[ServiceAttribute::NAME->value] = $serviceName;
        }

        return Resource::create($attributes);
    }

    private function getEnv(string $name) : ?string
    {
        $value = \getenv($name);

        if ($value === false || $value === '') {
            return null;
        }

        return $value;
    }

    /**
     * @return array<string, string>
     */
    private function parseResourceAttributes() : array
    {
        $rawAttributes = $this->getEnv(self::OTEL_RESOURCE_ATTRIBUTES);

        if ($rawAttributes === null) {
            return [];
        }

        $attributes = [];
        $pairs = $this->splitByComma($rawAttributes);

        foreach ($pairs as $pair) {
            $pair = \trim($pair);

            if ($pair === '') {
                continue;
            }

            $equalsPos = \strpos($pair, '=');

            if ($equalsPos === false) {
                continue;
            }

            $key = \trim(\substr($pair, 0, $equalsPos));
            $value = \trim(\substr($pair, $equalsPos + 1));

            if ($key === '') {
                continue;
            }

            $value = \stripslashes($value);
            $attributes[$key] = $value;
        }

        return $attributes;
    }

    /**
     * Split a string by commas, respecting escaped commas.
     *
     * @return array<string>
     */
    private function splitByComma(string $input) : array
    {
        $result = [];
        $current = '';
        $length = \strlen($input);
        $i = 0;

        while ($i < $length) {
            $char = $input[$i];

            if ($char === '\\' && $i + 1 < $length) {
                $current .= $char . $input[$i + 1];
                $i += 2;

                continue;
            }

            if ($char === ',') {
                $result[] = $current;
                $current = '';
                $i++;

                continue;
            }

            $current .= $char;
            $i++;
        }

        if ($current !== '' || \count($result) > 0) {
            $result[] = $current;
        }

        return $result;
    }
}
