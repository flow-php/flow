<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Migrations\Tests\Unit;

use Flow\PostgreSql\Migrations\Exception\MigrationException;
use Flow\PostgreSql\Migrations\Version;
use PHPUnit\Framework\TestCase;

final class VersionTest extends TestCase
{
    public function test_equals_with_different_version() : void
    {
        $version1 = Version::fromString('20260403120000');
        $version2 = Version::fromString('20260403130000');

        self::assertFalse($version1->equals($version2));
    }

    public function test_equals_with_same_version() : void
    {
        $version1 = Version::fromString('20260403120000');
        $version2 = Version::fromString('20260403120000');

        self::assertTrue($version1->equals($version2));
    }

    public function test_from_string_rejects_special_characters() : void
    {
        $this->expectException(MigrationException::class);

        Version::fromString('version-1.0');
    }

    public function test_from_string_with_alphanumeric_version() : void
    {
        $version = Version::fromString('abc123def456');

        self::assertSame('abc123def456', (string) $version);
    }

    public function test_from_string_with_empty_string() : void
    {
        $this->expectException(MigrationException::class);

        Version::fromString('');
    }

    public function test_from_string_with_numeric_version() : void
    {
        $version = Version::fromString('00001');

        self::assertSame('00001', (string) $version);
    }

    public function test_from_string_with_timestamp_version() : void
    {
        $version = Version::fromString('20260403120000');

        self::assertSame('20260403120000', (string) $version);
    }

    public function test_from_string_with_underscore_version() : void
    {
        $version = Version::fromString('2026_0403_120000');

        self::assertSame('2026_0403_120000', (string) $version);
    }

    public function test_is_after_returns_false_for_earlier_version() : void
    {
        $earlier = Version::fromString('20260401120000');
        $later = Version::fromString('20260403120000');

        self::assertFalse($earlier->isAfter($later));
    }

    public function test_is_after_returns_false_for_same_version() : void
    {
        $version = Version::fromString('20260403120000');

        self::assertFalse($version->isAfter(Version::fromString('20260403120000')));
    }

    public function test_is_after_returns_true_for_later_version() : void
    {
        $earlier = Version::fromString('20260401120000');
        $later = Version::fromString('20260403120000');

        self::assertTrue($later->isAfter($earlier));
    }

    public function test_to_string() : void
    {
        $version = Version::fromString('20260403120000');

        self::assertSame('20260403120000', (string) $version);
    }
}
