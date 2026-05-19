<?php

declare(strict_types=1);

namespace Flow\Bridge\Psr3\Telemetry\Tests\Unit;

use DateTimeImmutable;
use Flow\Bridge\Psr3\Telemetry\ValueNormalizer;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use stdClass;
use Stringable;

final class ValueNormalizerTest extends TestCase
{
    public function test_normalizes_arrays_recursively(): void
    {
        $normalizer = new ValueNormalizer();

        static::assertSame(
            ['a' => 1, 'b' => ['c' => 'two']],
            $normalizer->normalize(['a' => 1, 'b' => ['c' => 'two']]),
        );
    }

    public function test_normalizes_datetime_passthrough(): void
    {
        $normalizer = new ValueNormalizer();
        $datetime = new DateTimeImmutable('2024-01-15 10:30:00');

        static::assertSame($datetime, $normalizer->normalize($datetime));
    }

    public function test_normalizes_null_to_string(): void
    {
        static::assertSame('null', (new ValueNormalizer())->normalize(null));
    }

    public function test_normalizes_object_with_to_string_to_string(): void
    {
        $object = new class implements Stringable {
            public function __toString(): string
            {
                return 'rendered';
            }
        };

        static::assertSame('rendered', (new ValueNormalizer())->normalize($object));
    }

    public function test_normalizes_object_without_to_string_to_class_name(): void
    {
        static::assertSame(stdClass::class, (new ValueNormalizer())->normalize(new stdClass()));
    }

    public function test_normalizes_resource_to_debug_type(): void
    {
        $resource = fopen('php://memory', 'rb');
        static::assertNotFalse($resource);

        try {
            static::assertSame('resource (stream)', (new ValueNormalizer())->normalize($resource));
        } finally {
            fclose($resource);
        }
    }

    public function test_normalizes_scalar_passthrough(): void
    {
        $normalizer = new ValueNormalizer();

        static::assertSame('text', $normalizer->normalize('text'));
        static::assertSame(42, $normalizer->normalize(42));
        static::assertSame(3.14, $normalizer->normalize(3.14));
        static::assertTrue($normalizer->normalize(true));
    }

    public function test_normalizes_throwable_passthrough(): void
    {
        $exception = new RuntimeException('boom');

        static::assertSame($exception, (new ValueNormalizer())->normalize($exception));
    }
}
