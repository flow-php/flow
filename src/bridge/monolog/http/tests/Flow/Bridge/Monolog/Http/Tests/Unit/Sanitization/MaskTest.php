<?php

declare(strict_types=1);

namespace Flow\Bridge\Monolog\Http\Tests\Unit\Sanitization;

use Flow\Bridge\Monolog\Http\Exception\InvalidArgumentException;
use Flow\Bridge\Monolog\Http\Sanitization\Mask;
use Flow\ETL\Tests\FlowTestCase;

final class MaskTest extends FlowTestCase
{
    public function test_from_array_throws_exception_when_character_is_not_a_string() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Character must be a string');

        Mask::fromArray([
            'character' => 123,
        ]);
    }

    public function test_from_array_throws_exception_when_offset_is_not_an_integer() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Offset must be an integer');

        Mask::fromArray([
            'offset' => '2',
        ]);
    }

    public function test_from_array_with_custom_values() : void
    {
        $mask = Mask::fromArray([
            'character' => '#',
            'offset' => 2,
        ]);

        self::assertEquals('#', $mask->normalize()['character']);
        self::assertEquals(2, $mask->normalize()['offset']);
        self::assertEquals('ab###', $mask->sanitize('abcde'));
    }

    public function test_from_array_with_default_values() : void
    {
        $mask = Mask::fromArray([]);

        self::assertEquals('*', $mask->normalize()['character']);
        self::assertEquals(0, $mask->normalize()['offset']);
        self::assertEquals('*****', $mask->sanitize('abcde'));
    }

    public function test_normalize_returns_correct_array() : void
    {
        $mask = new Mask('#', 2);
        $normalized = $mask->normalize();

        self::assertEquals([
            'type' => 'mask',
            'character' => '#',
            'offset' => 2,
        ], $normalized);
    }

    public function test_roundtrip_conversion() : void
    {
        $original = new Mask('#', 2);
        $normalized = $original->normalize();
        $reconstructed = Mask::fromArray($normalized);

        self::assertEquals($original->normalize(), $reconstructed->normalize());
        self::assertEquals('ab###', $reconstructed->sanitize('abcde'));
    }

    public function test_sanitize_empty_string() : void
    {
        $mask = new Mask();

        self::assertEquals('', $mask->sanitize(''));
    }

    public function test_sanitize_with_custom_character() : void
    {
        $mask = new Mask('#');

        self::assertEquals('#####', $mask->sanitize('12345'));
        self::assertEquals('#####', $mask->sanitize('abcde'));
        self::assertEquals('##########', $mask->sanitize('1234567890'));
    }

    public function test_sanitize_with_default_character() : void
    {
        $mask = new Mask();

        self::assertEquals('*****', $mask->sanitize('12345'));
        self::assertEquals('*****', $mask->sanitize('abcde'));
        self::assertEquals('**********', $mask->sanitize('1234567890'));
    }

    public function test_sanitize_with_offset() : void
    {
        $mask = new Mask('*', 2);

        self::assertEquals('12***', $mask->sanitize('12345'));
        self::assertEquals('ab***', $mask->sanitize('abcde'));
        self::assertEquals('12********', $mask->sanitize('1234567890'));
    }

    public function test_sanitize_with_offset_larger_than_value_length() : void
    {
        $mask = new Mask('*', 10);

        self::assertEquals('12345', $mask->sanitize('12345'));
        self::assertEquals('abcde', $mask->sanitize('abcde'));
        self::assertEquals('1234567890', $mask->sanitize('1234567890'));
    }
}
