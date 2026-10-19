<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Filter\Binding;

use Flow\Parquet\Data\PhysicalOrder;
use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\Filter\Binding\Absent;
use Flow\Parquet\Filter\Binding\Bound;
use Flow\Parquet\Filter\Binding\BoundFilter;
use Flow\Parquet\Filter\Binding\PhysicalInterval;
use Flow\Parquet\Filter\Binding\PhysicalIntervals;
use Flow\Parquet\Filter\Binding\Unbound;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\ListElement;
use Flow\Parquet\ParquetFile\Schema\NestedColumn;
use Flow\Parquet\Tests\Mother\FlatColumnMother;
use PHPUnit\Framework\TestCase;

use function array_map;
use function Flow\Parquet\DSL\parquet_all;
use function Flow\Parquet\DSL\parquet_any;
use function Flow\Parquet\DSL\parquet_column;
use function Flow\Parquet\DSL\parquet_not;

final class BoundFilterTest extends TestCase
{
    public function test_each_leaf_binds_to_its_column_with_its_pre_image(): void
    {
        $id = FlatColumn::int64('id');
        $city = FlatColumn::string('city');
        $schema = Schema::with($id, NestedColumn::struct('address', [$city]));
        $equals = parquet_column('id')->equals(5);
        $prefix = parquet_column('address.city')->startsWith('Wa');
        $null = parquet_column('id')->isNull();
        $set = parquet_column('id')->in([1]);
        $bound = BoundFilter::bind(
            parquet_all($equals, parquet_not(parquet_any($prefix, $null)), $set),
            $schema,
            new Options(),
        );

        static::assertEquals(
            new Bound($schema->getFlat('id'), PhysicalOrder::Signed, new PhysicalIntervals([new PhysicalInterval(
                5,
                true,
                5,
                true,
            )])),
            $bound->leaf($equals),
        );
        static::assertEquals(
            new Bound(
                $schema->getFlat('address.city'),
                PhysicalOrder::Bytes,
                new PhysicalIntervals([new PhysicalInterval('Wa', true, 'Wb', false)]),
            ),
            $bound->leaf($prefix),
        );
        static::assertEquals(
            new Bound(
                $schema->getFlat('id'),
                PhysicalOrder::Signed,
                new PhysicalIntervals([PhysicalInterval::unbounded()]),
            ),
            $bound->leaf($null),
        );
        static::assertEquals(
            new Bound($schema->getFlat('id'), PhysicalOrder::Signed, new PhysicalIntervals([new PhysicalInterval(
                1,
                true,
                1,
                true,
            )])),
            $bound->leaf($set),
        );
        static::assertEquals([$schema->getFlat('id'), $schema->getFlat('address.city')], $bound->columns());
    }

    public function test_a_column_the_file_lacks_is_absent(): void
    {
        $leaf = parquet_column('missing')->equals(1);
        $bound = BoundFilter::bind($leaf, Schema::with(FlatColumn::int64('id')), new Options());

        static::assertEquals(new Absent(), $bound->leaf($leaf));
        static::assertSame([], $bound->columns());
    }

    public function test_a_leaf_the_file_cannot_answer_exactly_is_unbound(): void
    {
        $schema = Schema::with(
            FlatColumn::int64('id'),
            FlatColumnMother::int96(),
            NestedColumn::list('tags', ListElement::string()),
            NestedColumn::struct('address', [FlatColumn::string('city')]),
            FlatColumnMother::byteArrayDecimal(),
            FlatColumn::uuid('u'),
            FlatColumnMother::uint64('big'),
            FlatColumn::dateTime('ts'),
            FlatColumn::boolean('flag'),
        );
        $leaves = [
            parquet_column('id')->equals('1'),
            parquet_column('t')->isNull(),
            parquet_column('tags.list.element')->equals('a'),
            parquet_column('address')->isNull(),
            parquet_column('id')->startsWith('1'),
            parquet_column('d')->startsWith('1'),
            parquet_column('u')->startsWith('1'),
            parquet_column('big')->equals(' 9'),
            parquet_column('big')->in(['1', '09']),
            parquet_column('ts')->greaterThan('2024-01-01'),
            parquet_column('flag')->equals(1),
        ];
        $bound = BoundFilter::bind(parquet_all(...$leaves), $schema, new Options());

        foreach ($leaves as $leaf) {
            static::assertEquals(new Unbound(), $bound->leaf($leaf));
        }

        static::assertSame([], $bound->columns());
    }

    public function test_a_fraction_on_an_integer_column_binds_between_two_integers(): void
    {
        $schema = Schema::with(FlatColumn::int64('id'));
        $greater = parquet_column('id')->greaterThan(2.5);
        $set = parquet_column('id')->in([1, 2.5]);
        $bound = BoundFilter::bind(parquet_all($greater, $set), $schema, new Options());

        static::assertEquals(
            new Bound($schema->getFlat('id'), PhysicalOrder::Signed, new PhysicalIntervals([new PhysicalInterval(
                2,
                false,
                null,
                false,
            )])),
            $bound->leaf($greater),
        );
        static::assertEquals(
            new Bound($schema->getFlat('id'), PhysicalOrder::Signed, new PhysicalIntervals([new PhysicalInterval(
                1,
                true,
                1,
                true,
            )])),
            $bound->leaf($set),
        );
    }

    public function test_an_uppercase_uuid_binds_as_its_lowercase_form(): void
    {
        $schema = Schema::with(FlatColumn::uuid('u'));
        $upper = parquet_column('u')->equals('ABCDEF00-0000-4000-8000-000000000000');
        $lower = parquet_column('u')->equals('abcdef00-0000-4000-8000-000000000000');

        static::assertEquals(
            BoundFilter::bind($lower, $schema, new Options())->leaf($lower),
            BoundFilter::bind($upper, $schema, new Options())->leaf($upper),
        );
        static::assertInstanceOf(Bound::class, BoundFilter::bind($upper, $schema, new Options())->leaf($upper));
    }

    public function test_the_filter_is_kept_as_given(): void
    {
        $filter = parquet_column('id')->equals(1);

        static::assertSame(
            $filter,
            BoundFilter::bind($filter, Schema::with(FlatColumn::int64('id')), new Options())->filter(),
        );
    }

    public function test_a_leaf_of_another_tree_is_refused(): void
    {
        $bound = BoundFilter::bind(
            parquet_column('id')->equals(1),
            Schema::with(FlatColumn::int64('id')),
            new Options(),
        );

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('The leaf is not part of the bound filter');

        $bound->leaf(parquet_column('id')->equals(1));
    }

    public function test_the_row_filter_of_a_wholly_bound_filter_is_the_filter_itself(): void
    {
        $bound = BoundFilter::bind(
            parquet_all(parquet_column('id')->equals(1), parquet_column('missing')->isNull()),
            Schema::with(FlatColumn::int64('id')),
            new Options(),
        );

        static::assertSame($bound, $bound->rowFilter());
        static::assertNull($bound->residual());
    }

    public function test_the_row_filter_keeps_the_root_conjuncts_whose_leaves_are_all_bound(): void
    {
        $id = parquet_column('id')->equals(1);
        $name = parquet_column('name')->startsWith('a');
        $unbound = parquet_column('id')->equals('1');
        $mixed = parquet_any(parquet_column('name')->isNull(), parquet_column('id')->equals('2'));
        $bound = BoundFilter::bind(
            parquet_all(parquet_all($id, $unbound), parquet_not($name), $mixed),
            Schema::with(FlatColumn::int64('id'), FlatColumn::string('name')),
            new Options(),
        );

        $rowFilter = $bound->rowFilter();

        static::assertNotNull($rowFilter);
        static::assertEquals(parquet_all($id, parquet_not($name)), $rowFilter->filter());
        static::assertSame($bound->leaf($id), $rowFilter->leaf($id));
        static::assertSame($bound->leaf($name), $rowFilter->leaf($name));
        static::assertSame(
            ['id', 'name'],
            array_map(static fn(FlatColumn $column): string => $column->flatPath(), $rowFilter->columns()),
        );
        static::assertEquals(parquet_all($unbound, $mixed), $bound->residual());
    }

    public function test_a_single_bound_conjunct_is_the_row_filter_without_a_conjunction(): void
    {
        $id = parquet_column('id')->lessThan(5);
        $unbound = parquet_column('id')->equals('x');
        $bound = BoundFilter::bind(parquet_all($id, $unbound), Schema::with(FlatColumn::int64('id')), new Options());

        static::assertSame($id, $bound->rowFilter()?->filter());
        static::assertSame(
            ['id'],
            array_map(
                static fn(FlatColumn $column): string => $column->flatPath(),
                $bound->rowFilter()?->columns() ?? [],
            ),
        );
        static::assertSame($unbound, $bound->residual());
    }

    public function test_bound_is_the_binding_of_a_bound_or_absent_leaf_and_refuses_an_unbound_one(): void
    {
        $id = parquet_column('id')->equals(1);
        $missing = parquet_column('missing')->isNull();
        $unbound = parquet_column('id')->equals('1');
        $bound = BoundFilter::bind(
            parquet_all($id, $missing, $unbound),
            Schema::with(FlatColumn::int64('id')),
            new Options(),
        );

        static::assertSame($bound->leaf($id), $bound->bound($id));
        static::assertSame($bound->leaf($missing), $bound->bound($missing));

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Rows are evaluated on bound leaves only, the leaf on "id" is not bound');

        $bound->bound($unbound);
    }

    public function test_the_row_filter_of_a_lone_leaf_is_the_filter_itself_when_bound(): void
    {
        $bound = BoundFilter::bind(
            parquet_column('id')->equals(1),
            Schema::with(FlatColumn::int64('id')),
            new Options(),
        );

        static::assertSame($bound, $bound->rowFilter());
    }

    public function test_no_row_filter_when_no_root_conjunct_is_wholly_bound(): void
    {
        $schema = Schema::with(FlatColumn::int64('id'));

        static::assertNull(BoundFilter::bind(parquet_column('id')->equals('1'), $schema, new Options())->rowFilter());
        static::assertNull(
            BoundFilter::bind(
                parquet_any(parquet_column('id')->equals(1), parquet_column('id')->equals('1')),
                $schema,
                new Options(),
            )->rowFilter(),
        );
    }
}
