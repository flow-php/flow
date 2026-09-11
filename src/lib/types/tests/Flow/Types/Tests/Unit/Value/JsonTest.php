<?php

declare(strict_types=1);

namespace Flow\Types\Tests\Unit\Value;

use Flow\Types\Exception\InvalidArgumentException;
use Flow\Types\Value\Json;
use PHPUnit\Framework\TestCase;

final class JsonTest extends TestCase
{
    public function test_construct_with_invalid_json_throws_exception(): void
    {
        $this->expectException(InvalidArgumentException::class);
        new Json('invalid-json');
    }

    public function test_construct_with_valid_json_array(): void
    {
        $json = new Json('[1, 2, 3]');

        static::assertSame('[1, 2, 3]', $json->toString());
        static::assertFalse($json->isObject());
    }

    public function test_construct_with_valid_json_object(): void
    {
        $json = new Json('{"key": "value"}');

        static::assertSame('{"key": "value"}', $json->toString());
        static::assertTrue($json->isObject());
    }

    public function test_from_array_creates_instance(): void
    {
        $json = Json::fromArray(['a' => 1, 'b' => 2]);

        static::assertSame(['a' => 1, 'b' => 2], $json->toArray());
    }

    public function test_from_array_with_empty_array(): void
    {
        $json = Json::fromArray([]);

        static::assertSame('[]', $json->toString());
        static::assertFalse($json->isObject());
    }

    public function test_from_array_with_empty_array_as_object(): void
    {
        $json = Json::fromArray([], asObject: true);

        static::assertSame('{}', $json->toString());
        static::assertTrue($json->isObject());
    }

    public function test_from_string_creates_instance(): void
    {
        $jsonString = '{"name": "test"}';
        $json = Json::fromString($jsonString);

        static::assertSame($jsonString, $json->toString());
    }

    public function test_is_equal_ignores_whitespace_differences(): void
    {
        $json1 = new Json('{"key":"value"}');
        $json2 = new Json('{"key": "value"}');

        static::assertTrue($json1->isEqual($json2));
    }

    public function test_is_equal_with_different_json(): void
    {
        $json1 = new Json('{"a": 1}');
        $json2 = new Json('{"b": 2}');

        static::assertFalse($json1->isEqual($json2));
    }

    public function test_is_equal_with_same_json(): void
    {
        $json1 = new Json('{"key": "value"}');
        $json2 = new Json('{"key": "value"}');

        static::assertTrue($json1->isEqual($json2));
    }

    public function test_is_valid_returns_false_for_invalid_json(): void
    {
        static::assertFalse(Json::isValid('not json'));
        static::assertFalse(Json::isValid(''));
        static::assertFalse(Json::isValid('{invalid}'));
    }

    public function test_is_valid_returns_false_for_scalar_json(): void
    {
        static::assertFalse(Json::isValid('null'));
        static::assertFalse(Json::isValid('"string"'));
        static::assertFalse(Json::isValid('123'));
    }

    public function test_is_valid_returns_true_for_valid_json(): void
    {
        static::assertTrue(Json::isValid('[]'));
        static::assertTrue(Json::isValid('{}'));
        static::assertTrue(Json::isValid('{"key": "value"}'));
        static::assertTrue(Json::isValid('[1, 2, 3]'));
    }

    public function test_json_object_rejects_non_string_keys(): void
    {
        static::assertSame('{"1":"a"}', Json::fromArray([1 => 'a'])->toString());

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('All keys of a JSON object must be strings');

        Json::fromArray([1 => 'a'], asObject: true);
    }

    public function test_json_serialize_returns_array(): void
    {
        $json = new Json('{"key": "value"}');

        static::assertSame(['key' => 'value'], $json->jsonSerialize());
    }

    public function test_stringable_implementation(): void
    {
        $json = new Json('{"key": "value"}');

        static::assertSame('{"key": "value"}', (string) $json);
    }

    public function test_to_array_decodes_correctly(): void
    {
        $json = new Json('{"nested": {"key": "value"}, "list": [1, 2, 3]}');

        static::assertSame(
            [
                'nested' => ['key' => 'value'],
                'list' => [1, 2, 3],
            ],
            $json->toArray(),
        );
    }

    public function test_to_array_returns_list_as_array(): void
    {
        $json = new Json('[1, 2, 3]');

        static::assertSame([1, 2, 3], $json->toArray());
    }
}
