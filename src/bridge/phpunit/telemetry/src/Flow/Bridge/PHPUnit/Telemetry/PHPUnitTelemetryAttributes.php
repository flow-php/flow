<?php

declare(strict_types=1);

namespace Flow\Bridge\PHPUnit\Telemetry;

/**
 * Custom keys only, under `flow.phpunit.`; official `test.*` keys live in SemConvAttributes.
 *
 * @see https://opentelemetry.io/docs/specs/semconv/general/naming/
 */
final class PHPUnitTelemetryAttributes
{
    public const string ATTR_SUITE_IS_ROOT = 'flow.phpunit.suite.is_root';

    public const string ATTR_SUITE_TEST_COUNT = 'flow.phpunit.suite.test_count';

    public const string ATTR_TEST_CLASS = 'flow.phpunit.test.class';

    public const string ATTR_TEST_ID = 'flow.phpunit.test.id';

    /**
     * Value expressed in bytes.
     */
    public const string ATTR_TEST_MEMORY_DELTA = 'flow.phpunit.test.memory.delta';

    /**
     * Value expressed in bytes.
     */
    public const string ATTR_TEST_MEMORY_PEAK = 'flow.phpunit.test.memory.peak';

    public const string ATTR_TEST_METHOD = 'flow.phpunit.test.method';
}
