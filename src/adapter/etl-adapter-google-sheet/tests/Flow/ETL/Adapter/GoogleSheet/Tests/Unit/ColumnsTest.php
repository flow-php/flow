<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\GoogleSheet\Tests\Unit;

use Flow\ETL\Adapter\GoogleSheet\Columns;
use Flow\ETL\Exception\InvalidArgumentException;
use PHPUnit\Framework\TestCase;

final class ColumnsTest extends TestCase
{
    public static function invalid_cases() : \Generator
    {
        yield 'empty sheet name' => [
            '', 'ABC', 'CBA',
            'Sheet name can\'t be empty',
        ];
        yield 'start column contains number' => [
            'Sheet', 'ABC1', 'CBA',
            'The column "ABC1" needs to contain only upper-case letters.',
        ];
        yield 'start column contains white space' => [
            'Sheet', 'AB C', 'CBA',
            'The column "AB C" needs to contain only upper-case letters.',
        ];
        yield 'start column contains unicode' => [
            'Sheet', 'ABĆ', 'CBA',
            'The column "ABĆ" needs to contain only upper-case letters.',
        ];
        yield 'end column contains number' => [
            'Sheet', 'ABC', 'CBA1',
            'The column "CBA1" needs to contain only upper-case letters.',
        ];
        yield 'end column contains white space' => [
            'Sheet', 'ABC', 'CB A',
            'The column "CB A" needs to contain only upper-case letters.',
        ];
        yield 'end column contains unicode' => [
            'Sheet', 'ABC', 'ĆB',
            'The column "ĆB" needs to contain only upper-case letters.',
        ];
        yield 'columns in valid orders' => [
            'Sheet', 'BA', 'AB',
            'The column that starts the range "BA" must not be after the end column "AB"',
        ];
    }

    /**
     * @dataProvider invalid_cases
     */
    public function test_assertions(string $sheetName, string $startColumn, string $endColumn, string $expectedExceptionMessage) : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage($expectedExceptionMessage);

        new Columns($sheetName, $startColumn, $endColumn);
    }
}
