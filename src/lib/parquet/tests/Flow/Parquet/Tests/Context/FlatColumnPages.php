<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Context;

use Flow\Parquet\Dremel\ColumnData\PagedFlatColumnValues;
use Flow\Parquet\Dremel\ColumnData\ReadFlatColumnValues;
use Flow\Parquet\Dremel\ColumnData\WriteFlatColumnValues;
use Flow\Parquet\Dremel\DremelShredder;
use Flow\Parquet\Dremel\Validator\ColumnDataValidator;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Data\DataConverter;
use Flow\Parquet\ParquetFile\Schema;
use Generator;

use function array_filter;
use function array_slice;
use function count;

/**
 * Shredded rows read back as column chunks: one page per chunk, or two pages cut at a row boundary.
 */
final class FlatColumnPages
{
    /**
     * @param array<array<string, mixed>> $rows
     *
     * @return list<ReadFlatColumnValues>
     */
    public static function merged(Schema $schema, array $rows): array
    {
        $pages = [];

        foreach (self::shredded($schema, $rows) as $written) {
            $pages[] = self::page(
                $written,
                $written->repetitionLevels(),
                $written->definitionLevels(),
                $written->values(),
            );
        }

        return $pages;
    }

    /**
     * @param array<array<string, mixed>> $rows
     *
     * @return list<PagedFlatColumnValues>
     */
    public static function paged(Schema $schema, array $rows, int $firstPageRows): array
    {
        $paged = [];

        foreach (self::shredded($schema, $rows) as $written) {
            $paged[] = new PagedFlatColumnValues($written->column, self::cut($written, $firstPageRows));
        }

        return $paged;
    }

    /**
     * @param array<array<string, mixed>> $rows
     *
     * @return array<string, WriteFlatColumnValues>
     */
    public static function shredded(Schema $schema, array $rows): array
    {
        return (new DremelShredder(new ColumnDataValidator(), DataConverter::initialize(Options::default())))->shred(
            $schema,
            $rows,
            0,
        );
    }

    /**
     * @return Generator<ReadFlatColumnValues>
     */
    public static function cut(WriteFlatColumnValues $written, int $firstPageRows): Generator
    {
        $at = 0;
        $rows = 0;

        foreach ($written->repetitionLevels() as $level) {
            if ($level === 0 && $rows++ === $firstPageRows) {
                break;
            }

            $at++;
        }

        $maxDefinitionLevel = $written->column->repetitions()->maxDefinitionLevel();
        $values = count(array_filter(
            array_slice($written->definitionLevels(), 0, $at),
            static fn(int $level): bool => $level === $maxDefinitionLevel,
        ));

        yield self::page(
            $written,
            array_slice($written->repetitionLevels(), 0, $at),
            array_slice($written->definitionLevels(), 0, $at),
            array_slice($written->values(), 0, $values),
        );
        yield self::page(
            $written,
            array_slice($written->repetitionLevels(), $at),
            array_slice($written->definitionLevels(), $at),
            array_slice($written->values(), $values),
        );
    }

    /**
     * @param array<int> $repetitionLevels
     * @param array<int> $definitionLevels
     * @param array<mixed> $values
     */
    public static function page(
        WriteFlatColumnValues $written,
        array $repetitionLevels,
        array $definitionLevels,
        array $values,
    ): ReadFlatColumnValues {
        return new ReadFlatColumnValues(
            $written->column,
            (static function () use ($values): Generator {
                yield from $values;
            })(),
            $repetitionLevels,
            $definitionLevels,
        );
    }
}
