<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV\Tests\Context;

use Generator;

final class CSVRecordBoundaryContext
{
    /**
     * Buffers as CSVLineReader hands them over - joined lines, no trailing line end - and whether fgetcsv() would
     * consider the record closed.
     *
     * @return Generator<string, array{string, string, string, string, bool}>
     */
    public static function buffers(): Generator
    {
        yield 'no enclosure' => [',', '"', '\\', 'a,b', true];
        yield 'closed enclosure' => [',', '"', '\\', '"a",b', true];
        yield 'open enclosure' => [',', '"', '\\', '"a', false];
        yield 'open enclosure across a line' => [',', '"', '\\', "\"a\nb", false];
        yield 'closed across a line' => [',', '"', '\\', "\"a\nb\",c", true];
        yield 'escaped enclosure keeps it open' => [',', '"', '\\', '"x\"y', false];
        yield 'escape as the last byte' => [',', '"', '\\', '"x\\', false];
        yield 'doubled enclosure keeps it open' => [',', '"', '\\', '"a""', false];
        yield 'doubled then closed' => [',', '"', '\\', '"a"""', true];
        yield 'enclosure inside an unenclosed field' => [',', '"', '\\', 'x"y,1', true];
        yield 'blanks before an opening enclosure' => [',', '"', '\\', "a, \t\"b", false];
        yield 'carriage return before an opening enclosure' => [',', '"', '\\', "a,\r\"b", false];
        yield 'junk after a closing enclosure' => [',', '"', '\\', '"a"x"y', true];
        yield 'empty escape' => [',', '"', '', '"x\",1', true];
        yield 'escape equal to the enclosure' => [',', '"', '"', '"a""', false];
        yield 'custom separator and enclosure' => [';', "'", '\\', "x,'y;1", true];
        yield 'custom enclosure left open' => [';', "'", '\\', "a;'b", false];
        yield 'whitespace separator is not a blank' => ["\t", '"', '\\', "a\t\t\"b", false];
    }
}
