<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Migrations\Tests\Unit\VersionGenerator;

use Flow\PostgreSql\Migrations\VersionGenerator\TimestampVersionGenerator;
use PHPUnit\Framework\TestCase;

final class TimestampVersionGeneratorTest extends TestCase
{
    public function test_generates_14_digit_version_with_default_format() : void
    {
        $generator = new TimestampVersionGenerator();

        self::assertMatchesRegularExpression('/^\d{14}$/', (string) $generator->generate());
    }

    public function test_generates_version_with_custom_format() : void
    {
        $generator = new TimestampVersionGenerator('Ymd_His');

        self::assertMatchesRegularExpression('/^\d{8}_\d{6}$/', (string) $generator->generate());
    }

    public function test_generates_version_with_date_only_format() : void
    {
        $generator = new TimestampVersionGenerator('Ymd');

        self::assertMatchesRegularExpression('/^\d{8}$/', (string) $generator->generate());
    }
}
