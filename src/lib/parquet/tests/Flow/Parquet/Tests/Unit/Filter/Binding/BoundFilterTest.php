<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Filter\Binding;

use Flow\Parquet\Data\PhysicalOrder;
use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\Filter\All;
use Flow\Parquet\Filter\Binding\AbsentLeaf;
use Flow\Parquet\Filter\Binding\BoundFilter;
use Flow\Parquet\Filter\Binding\PhysicalInterval;
use Flow\Parquet\Filter\Binding\PhysicalIntervals;
use Flow\Parquet\Filter\Binding\RefusedLeaf;
use Flow\Parquet\Filter\Binding\ResolvedLeaf;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\ListElement;
use Flow\Parquet\ParquetFile\Schema\NestedColumn;
use Flow\Parquet\Tests\Mother\FlatColumnMother;
use PHPUnit\Framework\TestCase;

use function array_map;
use function Flow\Parquet\DSL\parquet_column;

final class BoundFilterTest extends TestCase
{
    public function test_each_leaf_resolves_to_its_column_with_its_pre_image(): void
    {
        $id = FlatColumn::int64('id');
        $city = FlatColumn::string('city');
        $schema = Schema::with($id, NestedColumn::struct('address', [$city]));
        $equals = parquet_column('id')->equals(5);
        $prefix = parquet_column('address.city')->startsWith('Wa');
        $null = parquet_column('id')->isNull();
        $set = parquet_column('id')->in([1]);
        $bound = BoundFilter::bind(new All($equals, $prefix->or($null)->negate(), $set), $schema, new Options());

        $equalsLeaf = $bound->leaf($equals);
        $prefixLeaf = $bound->leaf($prefix);
        $nullLeaf = $bound->leaf($null);
        $setLeaf = $bound->leaf($set);

        static::assertInstanceOf(ResolvedLeaf::class, $equalsLeaf);
        static::assertInstanceOf(ResolvedLeaf::class, $prefixLeaf);
        static::assertInstanceOf(ResolvedLeaf::class, $nullLeaf);
        static::assertInstanceOf(ResolvedLeaf::class, $setLeaf);
        static::assertEquals(
            new PhysicalIntervals(PhysicalOrder::Signed, [new PhysicalInterval(5, true, 5, true)]),
            $equalsLeaf->intervals,
        );
        static::assertEquals(
            new PhysicalIntervals(PhysicalOrder::Bytes, [new PhysicalInterval('Wa', true, 'Wb', false)]),
            $prefixLeaf->intervals,
        );
        static::assertEquals(new PhysicalIntervals(PhysicalOrder::Signed, []), $nullLeaf->intervals);
        static::assertEquals(
            new PhysicalIntervals(PhysicalOrder::Signed, [new PhysicalInterval(1, true, 1, true)]),
            $setLeaf->intervals,
        );
        static::assertEquals([$schema->getFlat('id'), $schema->getFlat('address.city')], $bound->columns());
        static::assertTrue($bound->answersEveryConjunct());
    }

    public function test_the_leaves_are_listed_depth_first(): void
    {
        $a = parquet_column('a')->equals(1);
        $b = parquet_column('b')->equals(1);
        $c = parquet_column('c')->isNull();
        $d = parquet_column('d')->in([1]);

        static::assertSame(
            [$a, $b, $c, $d],
            BoundFilter::bind(
                new All($a, $b->or($c->negate()), $d),
                Schema::with(FlatColumn::int64('a')),
                new Options(),
            )->leaves(),
        );
    }

    public function test_a_column_the_file_lacks_is_absent(): void
    {
        $leaf = parquet_column('missing')->equals(1);
        $bound = BoundFilter::bind($leaf, Schema::with(FlatColumn::int64('id')), new Options());

        static::assertEquals(new AbsentLeaf(), $bound->leaf($leaf));
        static::assertSame([], $bound->columns());
        static::assertTrue($bound->answersEveryConjunct());
    }

    public function test_a_leaf_the_file_cannot_answer_exactly_leaves_rows_unfiltered_by_it(): void
    {
        $schema = Schema::with(
            FlatColumn::int64('id'),
            NestedColumn::list('tags', ListElement::string()),
            NestedColumn::struct('address', [FlatColumn::string('city')]),
            FlatColumnMother::byteArrayDecimal(),
            FlatColumn::uuid('u'),
            FlatColumnMother::uint64('big'),
            FlatColumn::dateTime('ts'),
            FlatColumn::boolean('flag'),
            FlatColumn::json('doc'),
        );
        $leaves = [
            parquet_column('id')->equals('1'),
            parquet_column('tags.list.element')->equals('a'),
            parquet_column('address')->isNull(),
            parquet_column('id')->startsWith('1'),
            parquet_column('d')->startsWith('1'),
            parquet_column('u')->startsWith('1'),
            parquet_column('big')->equals(' 9'),
            parquet_column('big')->in(['1', '09']),
            parquet_column('ts')->greaterThan('2024-01-01'),
            parquet_column('flag')->equals(1),
            parquet_column('doc')->equals('{}'),
        ];
        $bound = BoundFilter::bind(new All(...$leaves), $schema, new Options());

        foreach ($leaves as $leaf) {
            $resolution = $bound->leaf($leaf);

            static::assertTrue(
                $resolution instanceof RefusedLeaf
                || $resolution instanceof ResolvedLeaf && $resolution->intervals === null,
            );
        }

        static::assertSame([], $bound->columns());
        static::assertNull($bound->rowFilter());
        static::assertFalse($bound->answersEveryConjunct());
    }

    public function test_is_null_on_an_int96_column_is_evaluated_on_rows(): void
    {
        $null = parquet_column('t')->isNull();
        $bound = BoundFilter::bind($null, Schema::with(FlatColumnMother::int96()), new Options());

        static::assertInstanceOf(ResolvedLeaf::class, $bound->rowLeaf($null));
        static::assertTrue($bound->answersEveryConjunct());
    }

    public function test_a_fraction_on_an_integer_column_binds_between_two_integers(): void
    {
        $schema = Schema::with(FlatColumn::int64('id'));
        $greater = parquet_column('id')->greaterThan(2.5);
        $set = parquet_column('id')->in([1, 2.5]);
        $bound = BoundFilter::bind($greater->and($set), $schema, new Options());

        $greaterLeaf = $bound->leaf($greater);
        $setLeaf = $bound->leaf($set);

        static::assertInstanceOf(ResolvedLeaf::class, $greaterLeaf);
        static::assertInstanceOf(ResolvedLeaf::class, $setLeaf);
        static::assertEquals(
            new PhysicalIntervals(PhysicalOrder::Signed, [new PhysicalInterval(2, false, null, false)]),
            $greaterLeaf->intervals,
        );
        static::assertEquals(
            new PhysicalIntervals(PhysicalOrder::Signed, [new PhysicalInterval(1, true, 1, true)]),
            $setLeaf->intervals,
        );
    }

    public function test_an_uppercase_uuid_binds_as_its_lowercase_form(): void
    {
        $schema = Schema::with(FlatColumn::uuid('u'));
        $upper = parquet_column('u')->equals('ABCDEF00-0000-4000-8000-000000000000');
        $lower = parquet_column('u')->equals('abcdef00-0000-4000-8000-000000000000');

        $upperLeaf = BoundFilter::bind($upper, $schema, new Options())->leaf($upper);

        static::assertEquals(BoundFilter::bind($lower, $schema, new Options())->leaf($lower), $upperLeaf);
        static::assertInstanceOf(ResolvedLeaf::class, $upperLeaf);
        static::assertNotNull($upperLeaf->intervals);
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

    public function test_the_row_filter_of_a_fully_bound_filter_is_the_filter_itself(): void
    {
        $bound = BoundFilter::bind(
            parquet_column('id')->equals(1)->and(parquet_column('missing')->isNull()),
            Schema::with(FlatColumn::int64('id')),
            new Options(),
        );

        static::assertSame($bound, $bound->rowFilter());
        static::assertNull($bound->residual());
        static::assertTrue($bound->answersEveryConjunct());
    }

    public function test_an_empty_all_is_fully_bound_without_a_row_filter(): void
    {
        $bound = BoundFilter::bind(new All(), Schema::with(FlatColumn::int64('id')), new Options());

        static::assertTrue($bound->answersEveryConjunct());
        static::assertNull($bound->rowFilter());
        static::assertNull($bound->residual());
    }

    public function test_the_row_filter_keeps_the_root_conjuncts_whose_leaves_are_all_answered(): void
    {
        $id = parquet_column('id')->equals(1);
        $name = parquet_column('name')->startsWith('a');
        $refused = parquet_column('id')->equals('1');
        $mixed = parquet_column('name')->isNull()->or(parquet_column('id')->equals('2'));
        $bound = BoundFilter::bind(
            new All($id->and($refused), $name->negate(), $mixed),
            Schema::with(FlatColumn::int64('id'), FlatColumn::string('name')),
            new Options(),
        );

        $rowFilter = $bound->rowFilter();

        static::assertNotNull($rowFilter);
        static::assertEquals($id->and($name->negate()), $rowFilter->filter());
        static::assertSame($bound->leaf($id), $rowFilter->leaf($id));
        static::assertSame($bound->leaf($name), $rowFilter->leaf($name));
        static::assertSame(
            ['id', 'name'],
            array_map(static fn(FlatColumn $column): string => $column->flatPath(), $rowFilter->columns()),
        );
        static::assertEquals($refused->and($mixed), $bound->residual()?->filter());
        static::assertFalse($bound->answersEveryConjunct());
    }

    public function test_a_single_answered_conjunct_is_the_row_filter_without_a_conjunction(): void
    {
        $id = parquet_column('id')->lessThan(5);
        $refused = parquet_column('id')->equals('x');
        $bound = BoundFilter::bind($id->and($refused), Schema::with(FlatColumn::int64('id')), new Options());

        static::assertSame($id, $bound->rowFilter()?->filter());
        static::assertSame(
            ['id'],
            array_map(
                static fn(FlatColumn $column): string => $column->flatPath(),
                $bound->rowFilter()?->columns() ?? [],
            ),
        );
        static::assertSame($refused, $bound->residual()?->filter());
    }

    public function test_a_row_leaf_is_an_answered_or_absent_leaf_and_another_is_refused(): void
    {
        $id = parquet_column('id')->equals(1);
        $missing = parquet_column('missing')->isNull();
        $refused = parquet_column('id')->equals('1');
        $bound = BoundFilter::bind(
            new All($id, $missing, $refused),
            Schema::with(FlatColumn::int64('id')),
            new Options(),
        );

        static::assertSame($bound->leaf($id), $bound->rowLeaf($id));
        static::assertSame($bound->leaf($missing), $bound->rowLeaf($missing));

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Rows are evaluated on leaves answered exactly, the leaf on "id" is not');

        $bound->rowLeaf($refused);
    }

    public function test_the_row_filter_of_a_lone_answered_leaf_is_the_filter_itself(): void
    {
        $bound = BoundFilter::bind(
            parquet_column('id')->equals(1),
            Schema::with(FlatColumn::int64('id')),
            new Options(),
        );

        static::assertSame($bound, $bound->rowFilter());
    }

    public function test_no_row_filter_when_no_root_conjunct_is_answered(): void
    {
        $schema = Schema::with(FlatColumn::int64('id'));

        static::assertNull(BoundFilter::bind(parquet_column('id')->equals('1'), $schema, new Options())->rowFilter());
        static::assertNull(
            BoundFilter::bind(
                parquet_column('id')->equals(1)->or(parquet_column('id')->equals('1')),
                $schema,
                new Options(),
            )->rowFilter(),
        );
    }
}
