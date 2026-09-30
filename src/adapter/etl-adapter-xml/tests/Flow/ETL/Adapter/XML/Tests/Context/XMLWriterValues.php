<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\XML\Tests\Context;

use Generator;

use function chr;

final class XMLWriterValues
{
    /**
     * @return Generator<string, array{string}>
     */
    public static function values(): Generator
    {
        yield 'markup characters' => ['a & b < c > d " e \' f'];
        yield 'an entity-looking text' => ['&amp; &lt;'];
        yield 'line breaks and a tab' => ["a\rb\nc\td"];
        yield 'a value cut at its first NUL' => ["a\0b"];
        yield 'only a NUL' => ["\0"];
        yield 'an empty string' => [''];
        yield 'a multi-byte string' => ['zażółć gęślą jaźń ☃'];
        yield 'plain text' => ['plain'];

        for ($byte = 1; $byte < 32; $byte++) {
            yield 'the control byte ' . $byte => ['a' . chr($byte) . 'b'];
        }
    }
}
