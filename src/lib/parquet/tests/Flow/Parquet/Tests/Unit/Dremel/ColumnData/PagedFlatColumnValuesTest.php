<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Dremel\ColumnData;

use Flow\Parquet\Dremel\DremelAssembler;
use Flow\Parquet\Dremel\ReadColumnData;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Data\DataConverter;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\ListElement;
use Flow\Parquet\ParquetFile\Schema\MapKey;
use Flow\Parquet\ParquetFile\Schema\MapValue;
use Flow\Parquet\ParquetFile\Schema\NestedColumn;
use Flow\Parquet\Tests\Context\FlatColumnPages;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

use function iterator_to_array;

final class PagedFlatColumnValuesTest extends TestCase
{
    /**
     * @return Generator<string, array{Schema, string, array<array<string, mixed>>}>
     */
    public static function nested(): Generator
    {
        yield 'list' => [
            Schema::with(NestedColumn::list('l', ListElement::int32())),
            'l',
            [['l' => [1, 2]], ['l' => null], ['l' => [3, null]], ['l' => []], ['l' => [4]]],
        ];
        yield 'map' => [
            Schema::with(NestedColumn::map('m', MapKey::string(), MapValue::int32())),
            'm',
            [['m' => ['a' => 1, 'b' => 2]], ['m' => null], ['m' => ['c' => null]], ['m' => ['d' => 4]]],
        ];
        yield 'struct' => [
            Schema::with(NestedColumn::struct('s', [FlatColumn::int32('a'), FlatColumn::string('b')])),
            's',
            [
                ['s' => ['a' => 1, 'b' => 'x']],
                ['s' => null],
                ['s' => ['a' => null, 'b' => 'y']],
                ['s' => [
                    'a' => 4,
                    'b' => null,
                ]],
            ],
        ];
    }

    /**
     * @param array<array<string, mixed>> $rows
     */
    #[DataProvider('nested')]
    public function test_two_pages_assemble_as_one_merged_page(Schema $schema, string $name, array $rows): void
    {
        $assembler = new DremelAssembler(DataConverter::initialize(Options::default()));
        $column = $schema->get($name);

        $merged = iterator_to_array($assembler->assemble(
            $column,
            new ReadColumnData($column, FlatColumnPages::merged($schema, $rows)),
        ));

        static::assertSame($rows, $merged);
        static::assertSame(
            $merged,
            iterator_to_array($assembler->assemble(
                $column,
                new ReadColumnData($column, FlatColumnPages::paged($schema, $rows, 2)),
            )),
        );
    }

    /**
     * @param array<array<string, mixed>> $rows
     */
    #[DataProvider('nested')]
    public function test_two_pages_iterate_as_one_merged_page(Schema $schema, string $name, array $rows): void
    {
        $merged = FlatColumnPages::merged($schema, $rows);
        $paged = FlatColumnPages::paged($schema, $rows, 2);

        foreach ($merged as $index => $page) {
            static::assertSame($page->flatPath(), $paged[$index]->flatPath());
            static::assertEquals(
                iterator_to_array($page->iterator(), false),
                iterator_to_array($paged[$index]->iterator()),
            );
        }
    }
}
