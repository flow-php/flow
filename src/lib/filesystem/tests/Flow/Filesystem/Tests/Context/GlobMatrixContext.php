<?php

declare(strict_types=1);

namespace Flow\Filesystem\Tests\Context;

use Generator;

final class GlobMatrixContext
{
    /**
     * @return list<string>
     */
    public static function files(): array
    {
        return [
            'data/.dir/x.parquet',
            'data/.hidden.parquet',
            'data/date=2026-09-01/one.parquet',
            'data/flat.parquet',
            'data/id=1/date=2026-09-01/two.parquet',
        ];
    }

    /**
     * @return Generator<string, array{string, list<string>}>
     */
    public static function patterns(): Generator
    {
        $all = self::files();

        yield 'star stays in one directory' => ['data/*.parquet', ['data/.hidden.parquet', 'data/flat.parquet']];
        yield 'double star inside a name is a star' => [
            'data/**.parquet',
            ['data/.hidden.parquet', 'data/flat.parquet'],
        ];
        yield 'double star segment spans zero or more directories' => ['data/**/*.parquet', $all];
        yield 'trailing double star is everything below' => ['data/**', $all];
        yield 'one directory level' => [
            'data/*/*.parquet',
            ['data/.dir/x.parquet', 'data/date=2026-09-01/one.parquet'],
        ];
        yield 'negated class' => [
            'data/**/[!f]*.parquet',
            [
                'data/.dir/x.parquet',
                'data/.hidden.parquet',
                'data/date=2026-09-01/one.parquet',
                'data/id=1/date=2026-09-01/two.parquet',
            ],
        ];
        yield 'caret is a literal in a class' => ['data/[^f]*.parquet', ['data/flat.parquet']];
        yield 'range' => ['data/[a-g]*.parquet', ['data/flat.parquet']];
        yield 'question mark never crosses a directory' => ['data/date=2026-09-01?one.parquet', []];
        yield 'partition placeholder' => ['data/date={date}/one.parquet', ['data/date=2026-09-01/one.parquet']];
        yield 'double star glued to a name stays in one directory' => ['data/x**/*.parquet', []];
        yield 'repeated separators address one path' => [
            'data//*.parquet',
            ['data/.hidden.parquet', 'data/flat.parquet'],
        ];
    }
}
