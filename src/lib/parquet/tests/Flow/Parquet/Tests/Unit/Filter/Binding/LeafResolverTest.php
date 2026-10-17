<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Filter\Binding;

use Flow\Parquet\Filter\Binding\AbsentLeaf;
use Flow\Parquet\Filter\Binding\LeafResolver;
use Flow\Parquet\Filter\Binding\LiteralDomain;
use Flow\Parquet\Filter\Binding\RefusedLeaf;
use Flow\Parquet\Filter\Binding\ResolvedLeaf;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\ListElement;
use Flow\Parquet\ParquetFile\Schema\NestedColumn;
use Flow\Parquet\Tests\Mother\FlatColumnMother;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

use function Flow\Parquet\DSL\parquet_all;
use function Flow\Parquet\DSL\parquet_column;
use function Flow\Parquet\DSL\parquet_not;

final class LeafResolverTest extends TestCase
{
    public function test_a_struct_leaf_resolves_to_its_root_column_and_field_path(): void
    {
        $city = NestedColumn::struct('address', [NestedColumn::struct('place', [FlatColumn::string('city')])]);
        $leaf = parquet_column('address.place.city')->equals('x');
        $schema = Schema::with($city);

        $resolution = LeafResolver::resolve(parquet_not($leaf), $schema, new Options())[$leaf];

        static::assertInstanceOf(ResolvedLeaf::class, $resolution);
        static::assertSame($schema->get('address.place.city'), $resolution->column);
        static::assertSame(LiteralDomain::Bytes, $resolution->domain);
        static::assertSame('address', $resolution->root);
        static::assertSame(['place', 'city'], $resolution->fields);
        static::assertSame(['x'], $resolution->literals);
    }

    public function test_the_leaves_are_resolved_in_the_filters_order_with_normalised_literals(): void
    {
        $first = parquet_column('u')->in(['ABCDEF00-0000-4000-8000-000000000000']);
        $second = parquet_column('missing')->isNull();
        $resolved = LeafResolver::resolve(
            parquet_all($first, $second),
            Schema::with(FlatColumn::uuid('u')),
            new Options(),
        );

        $leaves = [];

        foreach ($resolved as $leaf => $_) {
            $leaves[] = $leaf;
        }

        static::assertSame([$first, $second], $leaves);
        static::assertInstanceOf(AbsentLeaf::class, $resolved[$second]);
        static::assertInstanceOf(ResolvedLeaf::class, $resolved[$first]);
        static::assertSame(['abcdef00-0000-4000-8000-000000000000'], $resolved[$first]->literals);
    }

    #[TestWith(['big', ' 9', 'Column "big" (INT64 INTEGER(64, unsigned)) cannot hold the string literal \' 9\''])]
    #[TestWith(['amount', 'x', 'Column "amount" (INT64 DECIMAL(10, 2)) cannot hold the string literal \'x\''])]
    #[TestWith(['ts', 'x', 'Column "ts" (INT64 TIMESTAMP(MICROSECONDS, UTC)) cannot hold the string literal \'x\''])]
    #[TestWith(['flag', 1, 'Column "flag" (BOOLEAN) cannot hold the int literal 1'])]
    public function test_a_literal_the_column_cannot_hold_is_refused_with_the_full_column_type(
        string $column,
        int|string $literal,
        string $reason,
    ): void {
        $leaf = parquet_column($column)->equals($literal);

        static::assertEquals(
            new RefusedLeaf($reason),
            LeafResolver::resolve(
                $leaf,
                Schema::with(
                    FlatColumnMother::uint64('big'),
                    FlatColumn::decimal('amount', 10, 2),
                    FlatColumn::dateTime('ts'),
                    FlatColumn::boolean('flag'),
                ),
                new Options(),
            )[$leaf],
        );
    }

    public function test_a_struct_column_and_a_leaf_inside_a_list_are_refused(): void
    {
        $struct = parquet_column('address')->isNull();
        $listed = parquet_column('tags.list.element')->equals('a');
        $resolved = LeafResolver::resolve(
            parquet_all($struct, $listed),
            Schema::with(
                NestedColumn::struct('address', [FlatColumn::string('city')]),
                NestedColumn::list('tags', ListElement::string()),
            ),
            new Options(),
        );

        static::assertEquals(new RefusedLeaf('Filter column "address" is not a leaf column'), $resolved[$struct]);
        static::assertEquals(
            new RefusedLeaf(
                'Filter column "tags.list.element" lies inside a list or a map, where a row holds no single value',
            ),
            $resolved[$listed],
        );
    }

    public function test_a_prefix_on_a_column_without_strings_is_refused(): void
    {
        $leaf = parquet_column('i')->startsWith('1');

        static::assertEquals(
            new RefusedLeaf('Column "i" (INT64) holds no strings for a prefix'),
            LeafResolver::resolve($leaf, Schema::with(FlatColumn::int64('i')), new Options())[$leaf],
        );
    }
}
