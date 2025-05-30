<?php

declare(strict_types=1);

namespace Flow\Bridge\Monolog\Http\Tests\Unit\Sanitization;

use Flow\Bridge\Monolog\Http\Exception\InvalidArgumentException;
use Flow\Bridge\Monolog\Http\Sanitization\{Mask, SanitizerFactory};
use Flow\ETL\Tests\FlowTestCase;

final class SanitizerFactoryTest extends FlowTestCase
{
    public function test_creating_mask_sanitizer_from_array() : void
    {
        $sanitizer = SanitizerFactory::fromArray([
            'type' => 'mask',
            'character' => '#',
            'offset' => 2,
        ]);

        self::assertInstanceOf(Mask::class, $sanitizer);
        self::assertEquals('ab###', $sanitizer->sanitize('abcde'));
    }

    public function test_creating_mask_sanitizer_with_default_values() : void
    {
        $sanitizer = SanitizerFactory::fromArray([
            'type' => 'mask',
        ]);

        self::assertInstanceOf(Mask::class, $sanitizer);
        self::assertEquals('*****', $sanitizer->sanitize('abcde'));
    }

    public function test_roundtrip_conversion() : void
    {
        $original = new Mask('#', 2);
        $normalized = $original->normalize();
        $reconstructed = SanitizerFactory::fromArray($normalized);

        self::assertEquals($original->normalize(), $reconstructed->normalize());
        self::assertEquals('ab###', $reconstructed->sanitize('abcde'));
    }

    public function test_throws_exception_when_character_is_not_a_string() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Character must be a string');

        SanitizerFactory::fromArray([
            'type' => 'mask',
            'character' => 123,
        ]);
    }

    public function test_throws_exception_when_offset_is_not_an_integer() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Offset must be an integer');

        SanitizerFactory::fromArray([
            'type' => 'mask',
            'character' => '*',
            'offset' => '2',
        ]);
    }

    public function test_throws_exception_when_type_is_missing() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Sanitizer type is required');

        SanitizerFactory::fromArray([]);
    }

    public function test_throws_exception_when_type_is_unsupported() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Unsupported sanitizer type: unknown');

        SanitizerFactory::fromArray([
            'type' => 'unknown',
        ]);
    }
}
