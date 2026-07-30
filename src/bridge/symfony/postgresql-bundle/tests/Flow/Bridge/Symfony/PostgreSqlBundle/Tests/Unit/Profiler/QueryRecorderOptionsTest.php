<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSqlBundle\Tests\Unit\Profiler;

use Flow\Bridge\Symfony\PostgreSqlBundle\Profiler\QueryRecorderOptions;
use InvalidArgumentException;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

use function array_fill;

#[CoversClass(QueryRecorderOptions::class)]
final class QueryRecorderOptionsTest extends TestCase
{
    public function test_defaults(): void
    {
        $options = new QueryRecorderOptions();

        static::assertSame(1000, $options->maxQueries);
        static::assertTrue($options->includeParameters);
        static::assertSame(100, $options->maxRetainedParameters);
    }

    public function test_max_queries_wither_returns_new_instance(): void
    {
        $options = new QueryRecorderOptions();

        static::assertSame(500, $options->maxQueries(500)->maxQueries);
        static::assertSame(1000, $options->maxQueries);
    }

    public function test_include_parameters_wither_returns_new_instance(): void
    {
        $options = new QueryRecorderOptions();

        static::assertFalse($options->includeParameters(false)->includeParameters);
        static::assertTrue($options->includeParameters);
    }

    public function test_max_parameters_wither_returns_new_instance(): void
    {
        $options = new QueryRecorderOptions();

        static::assertSame(5, $options->maxRetainedParameters(5)->maxRetainedParameters);
        static::assertSame(100, $options->maxRetainedParameters);
    }

    #[TestWith([0])]
    #[TestWith([-1])]
    public function test_rejects_max_queries_below_one(int $maxQueries): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('QueryRecorderOptions::$maxQueries must be at least 1, got ' . $maxQueries);

        new QueryRecorderOptions(maxQueries: $maxQueries);
    }

    public function test_rejects_negative_max_parameters(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('QueryRecorderOptions::$maxRetainedParameters must not be negative, got -1');

        new QueryRecorderOptions(maxRetainedParameters: -1);
    }

    public function test_allows_null_max_parameters_as_unlimited(): void
    {
        static::assertNull((new QueryRecorderOptions(maxRetainedParameters: null))->maxRetainedParameters);
    }

    public function test_retains_parameters_within_limit(): void
    {
        static::assertTrue((new QueryRecorderOptions(maxRetainedParameters: 100))->retainsParameters(array_fill(
            0,
            5,
            'v',
        )));
    }

    public function test_retains_parameters_at_exact_limit(): void
    {
        static::assertTrue((new QueryRecorderOptions(maxRetainedParameters: 100))->retainsParameters(array_fill(
            0,
            100,
            'v',
        )));
    }

    public function test_does_not_retain_parameters_over_limit(): void
    {
        static::assertFalse((new QueryRecorderOptions(maxRetainedParameters: 100))->retainsParameters(array_fill(
            0,
            101,
            'v',
        )));
    }

    public function test_retains_everything_when_max_parameters_is_null(): void
    {
        static::assertTrue((new QueryRecorderOptions(maxRetainedParameters: null))->retainsParameters(array_fill(
            0,
            17_000,
            'v',
        )));
    }

    public function test_does_not_retain_parameters_when_include_parameters_disabled(): void
    {
        static::assertFalse((new QueryRecorderOptions(includeParameters: false))->retainsParameters([42]));
    }

    public function test_retains_empty_parameters_when_include_parameters_disabled(): void
    {
        static::assertTrue((new QueryRecorderOptions(includeParameters: false))->retainsParameters([]));
    }
}
