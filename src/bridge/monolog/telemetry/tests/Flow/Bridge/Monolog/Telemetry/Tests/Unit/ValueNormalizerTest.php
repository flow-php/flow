<?php

declare(strict_types=1);

namespace Flow\Bridge\Monolog\Telemetry\Tests\Unit;

use Flow\Bridge\Monolog\Telemetry\ValueNormalizer;
use PHPUnit\Framework\Attributes\{CoversClass, DataProvider};
use PHPUnit\Framework\TestCase;

#[CoversClass(ValueNormalizer::class)]
final class ValueNormalizerTest extends TestCase
{
    /**
     * @return \Generator<string, array{mixed, mixed}>
     */
    public static function scalarValuesProvider() : \Generator
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

    public function test_normalizes_anonymous_class_without_to_string() : void
    {
        $normalizer = new ValueNormalizer();
        $object = new class {
            public string $prop = 'value';
        };

        $result = $normalizer->normalize($object);

        self::assertIsString($result);
        self::assertStringContainsString('class@anonymous', $result);
    }

    public function test_normalizes_array_recursively() : void
    {
        $normalizer = new ValueNormalizer();
        $input = [
            'name' => 'John',
            'age' => 30,
            'active' => true,
        ];

        $result = $normalizer->normalize($input);

        self::assertIsArray($result);
        self::assertSame('John', $result['name']);
        self::assertSame(30, $result['age']);
        self::assertTrue($result['active']);
    }

    public function test_normalizes_array_with_null_values() : void
    {
        $normalizer = new ValueNormalizer();
        $input = [
            'value' => null,
            'other' => 'test',
        ];

        $result = $normalizer->normalize($input);

        self::assertIsArray($result);
        self::assertSame('null', $result['value']);
        self::assertSame('test', $result['other']);
    }

    public function test_normalizes_array_with_objects() : void
    {
        $normalizer = new ValueNormalizer();
        $object = new class {
            public function __toString() : string
            {
                return 'stringified';
            }
        };

        $input = [
            'obj' => $object,
            'std' => new \stdClass(),
        ];

        $result = $normalizer->normalize($input);

        self::assertIsArray($result);
        self::assertSame('stringified', $result['obj']);
        self::assertSame('stdClass', $result['std']);
    }

    public function test_normalizes_closed_resource_to_debug_type() : void
    {
        $normalizer = new ValueNormalizer();
        $resource = \fopen('php://memory', 'rb');
        self::assertIsResource($resource);
        \fclose($resource);

        $result = $normalizer->normalize($resource);

        self::assertSame('resource (closed)', $result);
    }

    public function test_normalizes_datetime_unchanged() : void
    {
        $normalizer = new ValueNormalizer();
        $datetime = new \DateTimeImmutable('2024-01-15 10:30:00');

        self::assertSame($datetime, $normalizer->normalize($datetime));
    }

    public function test_normalizes_error_unchanged() : void
    {
        $normalizer = new ValueNormalizer();
        $error = new \Error('Fatal error');

        self::assertSame($error, $normalizer->normalize($error));
    }

    public function test_normalizes_mutable_datetime_unchanged() : void
    {
        $normalizer = new ValueNormalizer();
        $datetime = new \DateTime('2024-01-15 10:30:00');

        self::assertSame($datetime, $normalizer->normalize($datetime));
    }

    public function test_normalizes_nested_arrays() : void
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

        self::assertIsArray($result);
        self::assertIsArray($result['user']);
        self::assertSame('John', $result['user']['name']);
        self::assertIsArray($result['user']['metadata']);
        self::assertSame('admin', $result['user']['metadata']['role']);
    }

    public function test_normalizes_null_to_string() : void
    {
        $normalizer = new ValueNormalizer();

        self::assertSame('null', $normalizer->normalize(null));
    }

    public function test_normalizes_object_with_to_string() : void
    {
        $normalizer = new ValueNormalizer();
        $object = new class {
            public function __toString() : string
            {
                return 'custom-string-value';
            }
        };

        self::assertSame('custom-string-value', $normalizer->normalize($object));
    }

    public function test_normalizes_object_without_to_string_to_class_name() : void
    {
        $normalizer = new ValueNormalizer();
        $object = new \stdClass();

        self::assertSame('stdClass', $normalizer->normalize($object));
    }

    public function test_normalizes_resource_to_debug_type() : void
    {
        $normalizer = new ValueNormalizer();
        $resource = \fopen('php://memory', 'rb');
        self::assertIsResource($resource);

        $result = $normalizer->normalize($resource);

        \fclose($resource);

        self::assertSame('resource (stream)', $result);
    }

    #[DataProvider('scalarValuesProvider')]
    public function test_normalizes_scalar_values_unchanged(mixed $input, mixed $expected) : void
    {
        $normalizer = new ValueNormalizer();

        self::assertSame($expected, $normalizer->normalize($input));
    }

    public function test_normalizes_throwable_unchanged() : void
    {
        $normalizer = new ValueNormalizer();
        $exception = new \RuntimeException('Something went wrong');

        self::assertSame($exception, $normalizer->normalize($exception));
    }
}
