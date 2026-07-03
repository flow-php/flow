<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Unit\Instrumentation\Doctrine\DBAL;

use DateTimeImmutable;
use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Doctrine\DBAL\ParameterFormatter;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

#[CoversClass(ParameterFormatter::class)]
final class ParameterFormatterTest extends TestCase
{
    public function test_formats_scalars_null_and_bool(): void
    {
        $formatted = (new ParameterFormatter())->format([42, null, true, 'x'], 10, 100);

        static::assertSame(
            [
                'db.query.parameter.0' => '42',
                'db.query.parameter.1' => 'NULL',
                'db.query.parameter.2' => 'true',
                'db.query.parameter.3' => 'x',
            ],
            $formatted,
        );
    }

    public function test_formats_named_parameters(): void
    {
        $formatted = (new ParameterFormatter())->format(['name' => 'John'], 10, 100);

        static::assertSame(['db.query.parameter.name' => 'John'], $formatted);
    }

    public function test_caps_number_of_parameters(): void
    {
        $formatted = (new ParameterFormatter())->format([1, 2, 3, 4], 2, 100);

        static::assertSame(
            [
                'db.query.parameter.0' => '1',
                'db.query.parameter.1' => '2',
            ],
            $formatted,
        );
    }

    public function test_truncates_long_values(): void
    {
        $formatted = (new ParameterFormatter())->format(['abcdefghij'], 10, 4);

        static::assertSame(['db.query.parameter.0' => 'abcd...'], $formatted);
    }

    public function test_returns_empty_for_no_parameters(): void
    {
        static::assertSame([], (new ParameterFormatter())->format([], 10, 100));
    }

    public function test_does_not_truncate_when_max_length_is_zero(): void
    {
        $formatted = (new ParameterFormatter())->format(['abcdefghij'], 10, 0);

        static::assertSame(['db.query.parameter.0' => 'abcdefghij'], $formatted);
    }

    public function test_formats_datetime(): void
    {
        $formatted = (new ParameterFormatter())->format([new DateTimeImmutable('2026-01-01T00:00:00+00:00')], 10, 100);

        static::assertSame(['db.query.parameter.0' => '2026-01-01T00:00:00+00:00'], $formatted);
    }

    public function test_formats_arrays_as_json(): void
    {
        $formatted = (new ParameterFormatter())->format([['a' => 1]], 10, 100);

        static::assertSame(['db.query.parameter.0' => '{"a":1}'], $formatted);
    }

    public function test_formats_stringable_objects(): void
    {
        $stringable = new class {
            public function __toString(): string
            {
                return 'stringable';
            }
        };

        $formatted = (new ParameterFormatter())->format([$stringable], 10, 100);

        static::assertSame(['db.query.parameter.0' => 'stringable'], $formatted);
    }

    public function test_formats_plain_objects_as_json(): void
    {
        $object = new class {
            public int $id = 7;
        };

        $formatted = (new ParameterFormatter())->format([$object], 10, 100);

        static::assertSame(['db.query.parameter.0' => '{"id":7}'], $formatted);
    }
}
