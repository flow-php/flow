<?php

declare(strict_types=1);

namespace Flow\Bridge\Monolog\Http\Tests\Unit\Config;

use Flow\Bridge\Monolog\Http\Config\ResponseConfig;
use Flow\Bridge\Monolog\Http\Exception\InvalidArgumentException;
use Flow\Bridge\Monolog\Http\Sanitization\Sanitizer;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Types\Exception\InvalidTypeException;

final class ResponseConfigTest extends FlowTestCase
{
    public function test_constructor_accepts_sanitizer_instance(): void
    {
        $sanitizer = $this->createStub(Sanitizer::class);

        $config = new ResponseConfig(sanitizers: [
            'valid' => $sanitizer,
        ]);

        static::assertCount(1, $config->sanitizers());
    }

    public function test_constructor_accepts_valid_sanitizer_array(): void
    {
        $config = new ResponseConfig(sanitizers: [
            'valid' => ['type' => 'mask', 'character' => '#', 'offset' => 2],
        ]);

        static::assertCount(1, $config->sanitizers());
    }

    public function test_constructor_throws_exception_when_mask_sanitizer_has_invalid_character(): void
    {
        $this->expectException(InvalidTypeException::class);
        $this->expectExceptionMessage(
            'Expected type "structure{type: \'mask\', character?: string, offset?: integer}", got "structure{type: string, character: integer}"',
        );

        new ResponseConfig(sanitizers: [
            'invalid' => ['type' => 'mask', 'character' => 123],
        ]);
    }

    public function test_constructor_throws_exception_when_mask_sanitizer_has_invalid_offset(): void
    {
        $this->expectException(InvalidTypeException::class);
        $this->expectExceptionMessage(
            'Expected type "structure{type: \'mask\', character?: string, offset?: integer}", got "structure{type: string, character: string, offset: string}"',
        );

        new ResponseConfig(sanitizers: [
            'invalid' => ['type' => 'mask', 'character' => '*', 'offset' => '2'],
        ]);
    }

    public function test_constructor_throws_exception_when_sanitizer_array_has_invalid_type(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            'Sanitizer for key "invalid" could not be created from array: Unsupported sanitizer type: invalid',
        );

        new ResponseConfig(sanitizers: [
            'invalid' => ['type' => 'invalid'],
        ]);
    }

    public function test_constructor_throws_exception_when_sanitizer_array_is_invalid(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            'Sanitizer for key "invalid" could not be created from array: Sanitizer type is required',
        );

        new ResponseConfig(sanitizers: [
            'invalid' => [],
        ]);
    }
}
