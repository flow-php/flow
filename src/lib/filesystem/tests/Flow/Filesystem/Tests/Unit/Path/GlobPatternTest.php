<?php

declare(strict_types=1);

namespace Flow\Filesystem\Tests\Unit\Path;

use Flow\Filesystem\Path\GlobPattern;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class GlobPatternTest extends TestCase
{
    public static function patterns(): Generator
    {
        yield ['/data/*.csv', '/data/a.csv', true];
        yield ['/data/*.csv', '/data/x/a.csv', false];
        yield ['/data/*.csv', '/data/.a.csv', true];
        yield ['/data/*.csv', '/data/a.csvx', false];
        yield ['/data/?.csv', '/data/a.csv', true];
        yield ['/data/?.csv', '/data/ab.csv', false];
        yield ['/data?x.csv', '/data/x.csv', false];
        yield ['/data/[abc].csv', '/data/b.csv', true];
        yield ['/data/[a-c].csv', '/data/b.csv', true];
        yield ['/data/[a-c].csv', '/data/-.csv', false];
        yield ['/data/[!a].csv', '/data/b.csv', true];
        yield ['/data/[!a].csv', '/data/a.csv', false];
        yield ['/data/[^a].csv', '/data/^.csv', true];
        yield ['/data/[^a].csv', '/data/b.csv', false];
        yield ['/data/[]].csv', '/data/].csv', true];
        yield ['/data/[!]].csv', '/data/].csv', false];
        yield ['/data/[abc.csv', '/data/[abc.csv', true];
        yield ['/data/**.csv', '/data/a.csv', true];
        yield ['/data/**.csv', '/data/x/a.csv', false];
        yield ['/data/**/*.csv', '/data/a.csv', true];
        yield ['/data/**/*.csv', '/data/x/y/a.csv', true];
        yield ['/data/**/*.csv', '/data/.x/a.csv', true];
        yield ['/data/**', '/data/a.csv', true];
        yield ['/data/**', '/data/x/y/a.csv', true];
        yield ['/data/**/**/*.csv', '/data/a.csv', true];
        yield ['/data/x**/*.csv', '/data/xy/a.csv', true];
        yield ['/data/x**/*.csv', '/data/xy/z/a.csv', false];
        yield ['/output/{order-name}.csv', '/output/1-PL.csv', true];
        yield ['/output/{order-name}.csv', '/output/.csv', false];
        yield ['/output/{order-name}.csv', '/output/n/1.csv', false];
        yield ['/data/a.b', '/data/aXb', false];
        yield ['/data/(x)+.csv', '/data/(x)+.csv', true];
        yield ['C:/data/**/*.csv', 'C:/data/x/a.csv', true];
        yield ['*.csv', 'a.csv', true];
        yield ['*.csv', 'x/a.csv', false];
        yield ['**/*.csv', 'a.csv', true];
        yield ['**/*.csv', 'x/y/a.csv', true];
        yield ['/data[!a]x.csv', '/data/x.csv', false];
        yield ['/data/[\\]x.csv', '/data/\\x.csv', true];
        yield ['/data/[[:alpha:]]x', '/data/a]x', true];
        yield ['/data/[[:alpha:]]x', '/data/ax', false];
        yield ['/data/[z-a].csv', '/data/z.csv', false];
        yield ['/data/[!z-a].csv', '/data/b.csv', true];
        yield ['/data/[a/b]/x.csv', '/data/a/x.csv', false];
        yield ['/data/[a/b]/x.csv', '/data/[a/b]/x.csv', true];
        yield ['/data[.-0]a/x.csv', '/data/a/x.csv', false];
        yield ['/data[.-0]a/x.csv', '/data.a/x.csv', true];
        yield ['/data/?.csv', '/data/é.csv', true];
        yield ['/data/??.csv', '/data/é.csv', false];
        yield ['/data/[é].csv', '/data/é.csv', true];
        yield ['/data/[!a].csv', '/data/é.csv', true];
        yield ['/data/[à-é].csv', '/data/è.csv', true];
        yield ['/data/?.csv', "/data/\xff.csv", true];
        yield ["/data/\xff?.csv", "/data/\xffa.csv", true];
    }

    #[DataProvider('patterns')]
    public function test_matching(string $pattern, string $path, bool $expected): void
    {
        static::assertSame($expected, (new GlobPattern($pattern))->matches($path));
    }
}
