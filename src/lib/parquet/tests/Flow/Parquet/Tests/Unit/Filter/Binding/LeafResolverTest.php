<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Filter\Binding;

use Flow\Parquet\Data\PhysicalOrder;
use Flow\Parquet\Filter\Binding\AbsentLeaf;
use Flow\Parquet\Filter\Binding\LeafResolver;
use Flow\Parquet\Filter\Binding\LiteralDomain;
use Flow\Parquet\Filter\Binding\PhysicalInterval;
use Flow\Parquet\Filter\Binding\PhysicalIntervals;
use Flow\Parquet\Filter\Binding\RefusedLeaf;
use Flow\Parquet\Filter\Binding\ResolvedLeaf;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Data\DataConverter;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\ListElement;
use Flow\Parquet\ParquetFile\Schema\NestedColumn;
use Flow\Parquet\Tests\Mother\FlatColumnMother;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

use function Flow\Parquet\DSL\parquet_column;

final class LeafResolverTest extends TestCase
{
    public function test_a_struct_leaf_resolves_to_its_root_column_and_field_path(): void
    {
        $city = NestedColumn::struct('address', [NestedColumn::struct('place', [FlatColumn::string('city')])]);
        $schema = Schema::with($city);

        $resolution = LeafResolver::resolve(
            parquet_column('address.place.city')->equals('x'),
            $schema,
            DataConverter::initialize(new Options()),
        );

        static::assertInstanceOf(ResolvedLeaf::class, $resolution);
        static::assertSame($schema->get('address.place.city'), $resolution->column);
        static::assertSame(LiteralDomain::Bytes, $resolution->domain);
        static::assertSame('address', $resolution->root);
        static::assertSame(['place', 'city'], $resolution->fields);
        static::assertSame(['x'], $resolution->literals);
        static::assertEquals(
            new PhysicalIntervals(PhysicalOrder::Bytes, [new PhysicalInterval('x', true, 'x', true)]),
            $resolution->intervals,
        );
    }

    public function test_literals_are_normalised_and_a_column_the_file_lacks_is_absent(): void
    {
        $schema = Schema::with(FlatColumn::uuid('u'));
        $converters = DataConverter::initialize(new Options());
        $resolution = LeafResolver::resolve(
            parquet_column('u')->in(['ABCDEF00-0000-4000-8000-000000000000']),
            $schema,
            $converters,
        );

        static::assertInstanceOf(ResolvedLeaf::class, $resolution);
        static::assertSame(['abcdef00-0000-4000-8000-000000000000'], $resolution->literals);
        static::assertEquals(
            new AbsentLeaf(),
            LeafResolver::resolve(parquet_column('missing')->isNull(), $schema, $converters),
        );
    }

    public function test_is_null_holds_for_no_physical_value_even_on_an_unordered_column(): void
    {
        $isNull = LeafResolver::resolve(
            parquet_column('t')->isNull(),
            Schema::with(FlatColumnMother::int96()),
            DataConverter::initialize(new Options()),
        );

        static::assertInstanceOf(ResolvedLeaf::class, $isNull);
        static::assertEquals(new PhysicalIntervals(PhysicalOrder::Unordered, []), $isNull->intervals);
    }

    public function test_a_leaf_without_an_exact_pre_image_resolves_without_intervals(): void
    {
        $json = LeafResolver::resolve(
            parquet_column('doc')->equals('{}'),
            Schema::with(FlatColumn::json('doc')),
            DataConverter::initialize(new Options()),
        );

        static::assertInstanceOf(ResolvedLeaf::class, $json);
        static::assertNull($json->intervals);
    }

    #[TestWith(['big', ' 9', 'Column "big" (INT64 (INTEGER)) cannot hold the string literal \' 9\''])]
    #[TestWith(['amount', 'x', 'Column "amount" (INT64 (DECIMAL)) cannot hold the string literal \'x\''])]
    #[TestWith(['ts', 'x', 'Column "ts" (INT64 (TIMESTAMP)) cannot hold the string literal \'x\''])]
    #[TestWith(['flag', 1, 'Column "flag" (BOOLEAN) cannot hold the int literal 1'])]
    public function test_a_literal_the_column_cannot_hold_is_refused_with_the_column_type(
        string $column,
        int|string $literal,
        string $reason,
    ): void {
        static::assertEquals(
            new RefusedLeaf($reason),
            LeafResolver::resolve(
                parquet_column($column)->equals($literal),
                Schema::with(
                    FlatColumnMother::uint64('big'),
                    FlatColumn::decimal('amount', 10, 2),
                    FlatColumn::dateTime('ts'),
                    FlatColumn::boolean('flag'),
                ),
                DataConverter::initialize(new Options()),
            ),
        );
    }

    public function test_a_struct_column_and_a_leaf_inside_a_list_are_refused(): void
    {
        $schema = Schema::with(
            NestedColumn::struct('address', [FlatColumn::string('city')]),
            NestedColumn::list('tags', ListElement::string()),
        );
        $converters = DataConverter::initialize(new Options());

        static::assertEquals(
            new RefusedLeaf('Filter column "address" is not a leaf column'),
            LeafResolver::resolve(parquet_column('address')->isNull(), $schema, $converters),
        );
        static::assertEquals(
            new RefusedLeaf(
                'Filter column "tags.list.element" lies inside a list or a map, where a row holds no single value',
            ),
            LeafResolver::resolve(parquet_column('tags.list.element')->equals('a'), $schema, $converters),
        );
    }

    public function test_a_prefix_on_a_column_without_strings_is_refused(): void
    {
        static::assertEquals(
            new RefusedLeaf('Column "i" (INT64) holds no strings for a prefix'),
            LeafResolver::resolve(
                parquet_column('i')->startsWith('1'),
                Schema::with(FlatColumn::int64('i')),
                DataConverter::initialize(new Options()),
            ),
        );
    }
}
