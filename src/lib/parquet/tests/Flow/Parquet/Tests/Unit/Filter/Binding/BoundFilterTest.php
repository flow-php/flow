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
}
