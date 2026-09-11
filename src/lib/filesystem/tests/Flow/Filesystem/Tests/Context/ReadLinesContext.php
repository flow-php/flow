<?php

declare(strict_types=1);

namespace Flow\Filesystem\Tests\Context;

use Generator;

final class ReadLinesContext
{
    /**
     * @return Generator<string, array{string, non-empty-string, null|int<1, max>, list<string>}>
     */
    public static function cases(): Generator
    {
        yield 'empty stream' => ['', "\n", null, []];
        yield 'lone separator' => ["\n", "\n", null, ['']];
        yield 'trailing empty line' => ["x\n\n", "\n", null, ['x', '']];
        yield 'trailing separator' => ["x\ny\n", "\n", null, ['x', 'y']];
        yield 'no trailing separator' => ["x\ny", "\n", null, ['x', 'y']];
        yield 'empty middle line' => ["x\n\ny", "\n", null, ['x', '', 'y']];
        yield 'last line zero' => ["x\n0", "\n", null, ['x', '0']];
        yield 'multi-byte separator, trailing' => ['x||y||', '||', null, ['x', 'y']];
        yield 'multi-byte separator, once' => ['x||y', '||', null, ['x', 'y']];
        yield 'crlf separator' => ["x\r\ny", "\r\n", null, ['x', 'y']];
        yield 'separator split across chunks' => ['x||y||z', '||', 2, ['x', 'y', 'z']];
        yield 'crlf split across chunks' => ["ab\r\ncd", "\r\n", 3, ['ab', 'cd']];
        yield 'length is a read hint, not a line cap' => ["abcdefgh\nij", "\n", 4, ['abcdefgh', 'ij']];
        yield 'carriage return kept' => ["x\r\ny", "\n", null, ["x\r", 'y']];
    }

    /**
     * @return Generator<string, array{string, non-empty-string, null|int<1, max>, list<string>}>
     */
    public static function non_empty_cases(): Generator
    {
        foreach (self::cases() as $name => $case) {
            if ($case[0] !== '') {
                yield $name => $case;
            }
        }
    }
}
