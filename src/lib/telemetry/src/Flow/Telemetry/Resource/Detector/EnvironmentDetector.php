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
 * OTEL_SERVICE_NAME wins over service.name in OTEL_RESOURCE_ATTRIBUTES.
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
