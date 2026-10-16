<?php

declare(strict_types=1);

namespace Flow\ParquetViewer\Tests\Unit;

use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Data\DataConverter;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\ParquetViewer\StatisticFormatter;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

use function hex2bin;

final class StatisticFormatterTest extends TestCase
{
    /**
     * @return Generator<string, array{FlatColumn, bool|float|int|string|null, string}>
     */
    public static function statistics(): Generator
    {
        yield 'absent' => [FlatColumn::int64('i'), null, '-'];
        yield 'int' => [FlatColumn::int64('i'), 7, '7'];
        yield 'boolean' => [FlatColumn::boolean('b'), true, '1'];
        yield 'decimal' => [FlatColumn::decimal('d', 10, 2), -350, '-3.5'];
        yield 'datetime' => [FlatColumn::dateTime('t'), 1_500_000, '1970-01-01 00:00:01.500000'];
        yield 'time' => [FlatColumn::time('t'), 3_723_500_000, '01:02:03.500000'];
        yield 'uuid stays whole' => [
            FlatColumn::uuid('u'),
            (string) hex2bin('0f8fad5bd9cb469fa16570867728950e'),
            '0f8fad5b-d9cb-469f-a165-70867728950e',
        ];
        yield 'short string whole' => [FlatColumn::string('s'), 'the quick brown fox', 'the quick brown fox'];
        yield 'long string truncated at a word' => [
            FlatColumn::string('s'),
            'the quick brown fox jumps over',
            'the quick brown fox jumps...',
        ];
    }

    #[DataProvider('statistics')]
    public function test_format(FlatColumn $column, bool|float|int|string|null $physical, string $expected): void
    {
        static::assertSame($expected, (new StatisticFormatter(DataConverter::initialize(Options::default())))->format(
            $column,
            $physical,
        ));
    }
}
