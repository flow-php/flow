<?php

declare(strict_types=1);

namespace Flow\Bridge\Monolog\Http\Tests\Unit\Sanitization;

use Flow\Bridge\Monolog\Http\Sanitization\Mask;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Types\Exception\InvalidTypeException;

final class MaskTest extends FlowTestCase
{
    public function test_from_array_throws_exception_when_character_is_not_a_string(): void
    {
        $this->expectException(InvalidTypeException::class);
        $this->expectExceptionMessage(
            'Expected type "structure{type: \'mask\', character?: string, offset?: integer}", got "structure{type: string, character: integer}"',
        );

        Mask::fromArray([
            'type' => 'mask',
            'character' => 123,
        ]);
    }

    public function test_from_array_throws_exception_when_offset_is_not_an_integer(): void
    {
        $this->expectException(InvalidTypeException::class);
        $this->expectExceptionMessage(
            'Expected type "structure{type: \'mask\', character?: string, offset?: integer}", got "map<string, string>"',
        );

        Mask::fromArray([
            'type' => 'mask',
            'character' => '*',
            'offset' => '2',
        ]);
    }

    public function test_from_array_with_custom_values(): void
    {
        $mask = Mask::fromArray([
            'type' => 'mask',
            'character' => '#',
            'offset' => 2,
        ]);

        static::assertEquals('#', $mask->normalize()['character']);
        static::assertEquals(2, $mask->normalize()['offset']);
        static::assertEquals('ab###', $mask->sanitize('abcde'));
    }

    public function test_from_array_with_default_values(): void
    {
        $mask = Mask::fromArray([
            'type' => 'mask',
        ]);

        static::assertEquals('*', $mask->normalize()['character']);
        static::assertEquals(0, $mask->normalize()['offset']);
        static::assertEquals('*****', $mask->sanitize('abcde'));
    }

    public function test_normalize_returns_correct_array(): void
    {
        $mask = new Mask('#', 2);
        $normalized = $mask->normalize();

        static::assertEquals(
            [
                'type' => 'mask',
                'character' => '#',
                'offset' => 2,
            ],
            $normalized,
        );
    }

    public function test_roundtrip_conversion(): void
    {
        $original = new Mask('#', 2);
        $normalized = $original->normalize();
        $reconstructed = Mask::fromArray($normalized);

        static::assertEquals($original->normalize(), $reconstructed->normalize());
        static::assertEquals('ab###', $reconstructed->sanitize('abcde'));
    }

    public function test_sanitize_empty_string(): void
    {
        $mask = new Mask();

        static::assertEquals('', $mask->sanitize(''));
    }

    public function test_sanitize_with_custom_character(): void
    {
        $mask = new Mask('#');

        static::assertEquals('#####', $mask->sanitize('12345'));
        static::assertEquals('#####', $mask->sanitize('abcde'));
        static::assertEquals('##########', $mask->sanitize('1234567890'));
    }

    public function test_sanitize_with_default_character(): void
    {
        $mask = new Mask();

        static::assertEquals('*****', $mask->sanitize('12345'));
        static::assertEquals('*****', $mask->sanitize('abcde'));
        static::assertEquals('**********', $mask->sanitize('1234567890'));
    }

    public function test_sanitize_with_offset(): void
    {
        $mask = new Mask('*', 2);

        static::assertEquals('12***', $mask->sanitize('12345'));
        static::assertEquals('ab***', $mask->sanitize('abcde'));
        static::assertEquals('12********', $mask->sanitize('1234567890'));
    }

    public function test_sanitize_with_offset_larger_than_value_length(): void
    {
        $mask = new Mask('*', 10);

        static::assertEquals('12345', $mask->sanitize('12345'));
        static::assertEquals('abcde', $mask->sanitize('abcde'));
        static::assertEquals('1234567890', $mask->sanitize('1234567890'));
    }
}
