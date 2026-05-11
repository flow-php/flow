<?php

declare(strict_types=1);

namespace Flow\Bridge\Monolog\Telemetry\Tests\Unit;

use Flow\Bridge\Monolog\Telemetry\ValueNormalizer;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

#[CoversClass(ValueNormalizer::class)]
final class ValueNormalizerTest extends TestCase
{
    /**
     * @return \Generator<string, array{mixed, mixed}>
     */
    public static function scalarValuesProvider(): \Generator
    {
        yield 'string' => ['hello', 'hello'];
        yield 'empty string' => ['', ''];
        yield 'integer' => [42, 42];
        yield 'zero' => [0, 0];
        yield 'negative integer' => [-10, -10];
        yield 'float' => [3.14, 3.14];
        yield 'boolean true' => [true, true];
        yield 'boolean false' => [false, false];
    }

    public function test_normalizes_anonymous_class_without_to_string(): void
    {
        $normalizer = new ValueNormalizer();
        $object = new class {
            public string $prop = 'value';
        };

        $result = $normalizer->normalize($object);

        static::assertIsString($result);
        static::assertStringContainsString('class@anonymous', $result);
    }

    public function test_normalizes_array_recursively(): void
    {
        $normalizer = new ValueNormalizer();
        $input = [
            'name' => 'John',
            'age' => 30,
            'active' => true,
        ];

        $result = $normalizer->normalize($input);

        static::assertIsArray($result);
        static::assertSame('John', $result['name']);
        static::assertSame(30, $result['age']);
        static::assertTrue($result['active']);
    }

    public function test_normalizes_array_with_null_values(): void
    {
        $normalizer = new ValueNormalizer();
        $input = [
            'value' => null,
            'other' => 'test',
        ];

        $result = $normalizer->normalize($input);

        static::assertIsArray($result);
        static::assertSame('null', $result['value']);
        static::assertSame('test', $result['other']);
    }

    public function test_normalizes_array_with_objects(): void
    {
        $normalizer = new ValueNormalizer();
        $object = new class {
            public function __toString(): string
            {
                return 'stringified';
            }
        };

        $input = [
            'obj' => $object,
            'std' => new \stdClass(),
        ];

        $result = $normalizer->normalize($input);

        static::assertIsArray($result);
        static::assertSame('stringified', $result['obj']);
        static::assertSame('stdClass', $result['std']);
    }

    public function test_normalizes_closed_resource_to_debug_type(): void
    {
        $normalizer = new ValueNormalizer();
        $resource = \fopen('php://memory', 'rb');
        static::assertIsResource($resource);
        \fclose($resource);

        $result = $normalizer->normalize($resource);

        static::assertSame('resource (closed)', $result);
    }

    public function test_normalizes_datetime_unchanged(): void
    {
        $normalizer = new ValueNormalizer();
        $datetime = new \DateTimeImmutable('2024-01-15 10:30:00');

        static::assertSame($datetime, $normalizer->normalize($datetime));
    }

    public function test_normalizes_error_unchanged(): void
    {
        $normalizer = new ValueNormalizer();
        $error = new \Error('Fatal error');

        static::assertSame($error, $normalizer->normalize($error));
    }

    public function test_normalizes_mutable_datetime_unchanged(): void
    {
        $normalizer = new ValueNormalizer();
        $datetime = new \DateTime('2024-01-15 10:30:00');

        static::assertSame($datetime, $normalizer->normalize($datetime));
    }

    public function test_normalizes_nested_arrays(): void
    {
        $normalizer = new ValueNormalizer();
        $input = [
            'user' => [
                'name' => 'John',
                'metadata' => [
                    'role' => 'admin',
                ],
            ],
        ];

        $result = $normalizer->normalize($input);

        static::assertIsArray($result);
        static::assertIsArray($result['user']);
        static::assertSame('John', $result['user']['name']);
        static::assertIsArray($result['user']['metadata']);
        static::assertSame('admin', $result['user']['metadata']['role']);
    }

    public function test_normalizes_null_to_string(): void
    {
        $normalizer = new ValueNormalizer();

        static::assertSame('null', $normalizer->normalize(null));
    }

    public function test_normalizes_object_with_to_string(): void
    {
        $normalizer = new ValueNormalizer();
        $object = new class {
            public function __toString(): string
            {
                return 'custom-string-value';
            }
        };

        static::assertSame('custom-string-value', $normalizer->normalize($object));
    }

    public function test_normalizes_object_without_to_string_to_class_name(): void
    {
        $normalizer = new ValueNormalizer();
        $object = new \stdClass();

        static::assertSame('stdClass', $normalizer->normalize($object));
    }

    public function test_normalizes_resource_to_debug_type(): void
    {
        $normalizer = new ValueNormalizer();
        $resource = \fopen('php://memory', 'rb');
        static::assertIsResource($resource);

        $result = $normalizer->normalize($resource);

        \fclose($resource);

        static::assertSame('resource (stream)', $result);
    }

    #[DataProvider('scalarValuesProvider')]
    public function test_normalizes_scalar_values_unchanged(mixed $input, mixed $expected): void
    {
        $normalizer = new ValueNormalizer();

        static::assertSame($expected, $normalizer->normalize($input));
    }

    public function test_normalizes_throwable_unchanged(): void
    {
        $normalizer = new ValueNormalizer();
        $exception = new \RuntimeException('Something went wrong');

        static::assertSame($exception, $normalizer->normalize($exception));
    }
}
