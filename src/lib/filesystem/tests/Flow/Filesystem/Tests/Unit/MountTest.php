<?php

declare(strict_types=1);

namespace Flow\Filesystem\Tests\Unit;

use Flow\Filesystem\Exception\InvalidArgumentException;
use Flow\Filesystem\Mount;
use Flow\Filesystem\Path;
use PHPUnit\Framework\TestCase;

final class MountTest extends TestCase
{
    public function test_does_not_support_mismatching_path(): void
    {
        static::assertFalse((new Mount('warehouse'))->supports(Path::from('archive://data/file.csv')));
    }

    public function test_does_not_support_mismatching_string(): void
    {
        static::assertFalse((new Mount('warehouse'))->supports('archive'));
    }

    public function test_exposes_protocol(): void
    {
        static::assertSame('warehouse', (new Mount('warehouse'))->protocol);
    }

    public function test_rejects_empty_protocol(): void
    {
        $this->expectException(InvalidArgumentException::class);

        new Mount('');
    }

    public function test_rejects_invalid_characters(): void
    {
        $this->expectException(InvalidArgumentException::class);

        new Mount('1bad');
    }

    public function test_supports_matching_path(): void
    {
        static::assertTrue((new Mount('warehouse'))->supports(Path::from('warehouse://data/file.csv')));
    }

    public function test_supports_matching_string(): void
    {
        static::assertTrue((new Mount('warehouse'))->supports('warehouse'));
    }
}
