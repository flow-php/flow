<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Filesystem;

use Flow\ETL\Filesystem\Path;
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

    /**
     * @dataProvider double_dots_paths
     */
    public function test_double_dots_in_path(string $relative, string $absolute) : void
    {
        $this->assertEquals(new Path($absolute), Path::realpath($relative));
    }
}
