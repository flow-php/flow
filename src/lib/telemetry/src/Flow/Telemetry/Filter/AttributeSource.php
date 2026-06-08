<?php

declare(strict_types=1);

namespace Flow\Telemetry\Filter;

use Flow\Telemetry\Attributes;

use function array_map;

/**
 * Which attribute set on a signal an {@see AttributeFilter} inspects.
 *
 * {@see self::SIGNAL} - the signal's own attributes (span/metric/log record).
 * {@see self::RESOURCE} - the shared resource attributes (service.name, etc.).
 * {@see self::SCOPE} - the instrumentation scope attributes.
 */
enum AttributeSource
{
    case SIGNAL;
    case RESOURCE;
    case SCOPE;

    /**
     * Resolve each configured source to its attribute set, preserving order. The
     * single place the source-to-attributes mapping lives, shared by every consumer
     * that feeds {@see AttributeFilter::dropFunction()} (log/span/metric filters and
     * the attribute-matching sampler).
     *
     * @param non-empty-list<self> $sources
     *
     * @return non-empty-list<Attributes>
     */
    public static function select(array $sources, Attributes $signal, Attributes $resource, Attributes $scope): array
    {
        return array_map(static fn(self $source): Attributes => match ($source) {
            self::SIGNAL => $signal,
            self::RESOURCE => $resource,
            self::SCOPE => $scope,
        }, $sources);
    }
}
