<?php

declare(strict_types=1);

namespace Flow\Filesystem\Tests\Integration;

use Flow\Filesystem\Path;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class RealpathTest extends TestCase
{
    public static function double_dots_paths() : \Generator
    {
        yield ['/path/../file.txt', '/file.txt'];
        yield ['/path/./file.txt', '/path/file.txt'];
        yield ['/path/..//../file.txt', '/file.txt'];
        yield ['/path/more/nested/..//../file.txt', '/path/file.txt'];
    }

    #[DataProvider('double_dots_paths')]
    public function test_double_dots_in_path(string $relative, string $absolute) : void
    {
        self::assertEquals(new Path($absolute), Path::realpath($relative));
    }
}
