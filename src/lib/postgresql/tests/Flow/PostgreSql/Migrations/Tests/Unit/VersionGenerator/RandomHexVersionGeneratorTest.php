<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Migrations\Tests\Unit\VersionGenerator;

use Flow\PostgreSql\Migrations\VersionGenerator\RandomHexVersionGenerator;
use PHPUnit\Framework\TestCase;

final class RandomHexVersionGeneratorTest extends TestCase
{
    public function test_generates_hex_string_of_correct_length(): void
    {
        $generator = new RandomHexVersionGenerator(12);

        static::assertMatchesRegularExpression('/^[a-f0-9]{12}$/', (string) $generator->generate());
    }

    public function test_generates_hex_string_with_custom_length(): void
    {
        $generator = new RandomHexVersionGenerator(8);

        static::assertMatchesRegularExpression('/^[a-f0-9]{8}$/', (string) $generator->generate());
    }

    public function test_generates_unique_values(): void
    {
        $generator = new RandomHexVersionGenerator();

        static::assertNotSame((string) $generator->generate(), (string) $generator->generate());
    }
}
