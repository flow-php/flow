<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\DataFrame;

use DateTimeImmutable;
use Flow\ETL\DataFrame;
use Flow\ETL\Exception\SchemaDefinitionNotFoundException;
use Flow\ETL\Exception\SchemaNotDerivableException;
use Flow\ETL\Join\Join;
use Flow\ETL\Tests\Double\CountingExtractor;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Types\Exception\InvalidArgumentException;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use Throwable;

use function Flow\ETL\DSL\array_get;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\discover_pivot_values;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\from_data_frame;
use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\join_on;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\pivot_values;
use function Flow\ETL\DSL\rank;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\rename_replace;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\sum;
use function Flow\ETL\DSL\window;
use function Flow\ETL\DSL\with_entry;

final class PlanBindTest extends FlowTestCase
{
    public const array ROWS = [
        ['id' => 1, 'group' => 'a', 'amount' => 10, 'name' => 'x', 'payload' => ['colour' => 'red']],
        ['id' => 2, 'group' => 'b', 'amount' => 20, 'name' => 'y', 'payload' => ['colour' => 'blue']],
        ['id' => 3, 'group' => 'a', 'amount' => 30, 'name' => 'z', 'payload' => ['colour' => 'red']],
    ];

    /**
     * One plan per bind class of the step table. The two exclusions the plan names - a refused bind
     * (joinEach) and the BucketingProcessor metadata hop - are covered by their own tests.
     *
     * @return Generator<string, array{callable(): DataFrame}>
     */
    public static function plans(): Generator
    {
        yield 'select' => [static fn(): DataFrame => df()->read(from_array(self::ROWS))->select('id', 'name')];

        yield 'drop' => [static fn(): DataFrame => df()->read(from_array(self::ROWS))->drop('name', 'payload')];

        yield 'rename' => [static fn(): DataFrame => df()->read(from_array(self::ROWS))->rename('name', 'label')];

        yield 'renameEach' => [
            static fn(): DataFrame => df()->read(from_array(self::ROWS))->renameEach(rename_replace('am', 'AM')),
        ];

        yield 'filter' => [
            static fn(): DataFrame => df()->read(from_array(self::ROWS))->filter(ref('amount')->greaterThan(lit(10))),
        ];

        yield 'until' => [
            static fn(): DataFrame => df()->read(from_array(self::ROWS))->until(ref('amount')->lessThan(lit(30))),
        ];

        yield 'dropDuplicates' => [
            static fn(): DataFrame => df()->read(from_array(self::ROWS))->dropDuplicates(ref('group')),
        ];

        yield 'withEntry' => [
            static fn(): DataFrame => df()->read(from_array(self::ROWS))->withEntry('upper', ref('name')->upper()),
        ];

        yield 'unpack' => [
            static fn(): DataFrame => df()
                ->read(from_array(self::ROWS))
                ->withEntry('p', ref('payload')->unpack(schema(str_schema('colour'), int_schema('size')))),
        ];

        yield 'duplicateRow' => [
            static fn(): DataFrame => df()
                ->read(from_array(self::ROWS))
                ->duplicateRow(lit(true), with_entry('id', ref('id')->multiply(lit(-1)))),
        ];

        yield 'crossJoin' => [
            static fn(): DataFrame => df()
                ->read(from_array(self::ROWS))
                ->crossJoin(df()->read(from_array([['tag' => 't1'], ['tag' => 't2']]))),
        ];

        yield 'join' => [
            static fn(): DataFrame => df()
                ->read(from_array(self::ROWS))
                ->join(
                    df()->read(from_array([['group' => 'a', 'label' => 'A']])),
                    join_on(['group' => 'group']),
                    Join::left,
                ),
        ];

        yield 'groupBy+aggregate' => [
            static fn(): DataFrame => df()
                ->read(from_array(self::ROWS))
                ->groupBy([ref('group')])
                ->aggregate(sum(ref('amount'))),
        ];

        yield 'pivot' => [
            static fn(): DataFrame => df()
                ->read(from_array(self::ROWS))
                ->groupBy([ref('group')])
                ->pivot(ref('name'), pivot_values('x', 'y', 'z'))
                ->aggregate(sum(ref('amount'))),
        ];

        yield 'window' => [
            static fn(): DataFrame => df()
                ->read(from_array(self::ROWS))
                ->withEntry('position', rank()->over(window()->partitionBy(ref('group'))->orderBy(ref('amount')))),
        ];

        yield 'sortBy' => [static fn(): DataFrame => df()->read(from_array(self::ROWS))->sortBy(ref('amount'))];

        yield 'limit' => [static fn(): DataFrame => df()->read(from_array(self::ROWS))->limit(2)];

        yield 'offset' => [static fn(): DataFrame => df()->read(from_array(self::ROWS))->offset(1)];

        yield 'void' => [static fn(): DataFrame => df()->read(from_array(self::ROWS))->void()];

        yield 'collect' => [static fn(): DataFrame => df()->read(from_array(self::ROWS))->collect()];

        yield 'batchSize' => [static fn(): DataFrame => df()->read(from_array(self::ROWS))->batchSize(1)];
    }

    public function test_a_propagating_build_error_does_not_poison_the_memo(): void
    {
        $df = df()->read(from_array([['a' => 1, 'b' => 2]]));

        try {
            $df->select('nope')->schema();
            static::fail('select() on an undeclared column should refuse');
        } catch (SchemaDefinitionNotFoundException) {
            // the refusal is a build error, not a plan the bind gave up on
        }

        static::assertSame(
            ['a'],
            df()
                ->read(from_array([['a' => 1, 'b' => 2]]))
                ->select('a')
                ->schema()
                ->references()
                ->names(),
        );
    }

    public function test_a_refused_bind_rethrows_the_same_message_without_rebinding(): void
    {
        $df = df()->read($extractor = new CountingExtractor(schema(int_schema('a'))))->select('nope');

        $first = null;
        $second = null;

        try {
            $df->schema();
        } catch (SchemaDefinitionNotFoundException $e) {
            $first = $e;
        }

        try {
            $df->schema();
        } catch (SchemaDefinitionNotFoundException $e) {
            $second = $e;
        }

        static::assertNotNull($first);
        static::assertSame($first->getMessage(), $second?->getMessage());
        static::assertSame(0, $extractor->extractCalls);
    }

    public function test_adding_a_step_rebinds(): void
    {
        $df = df()->read(from_array([['a' => 1, 'b' => 2]]));

        static::assertSame(['a', 'b'], $df->schema()->references()->names());

        $df->drop('b');

        static::assertSame(['a'], $df->schema()->references()->names());
    }

    public function test_an_incomparable_column_pair_is_refused_at_bind(): void
    {
        $extractor = new CountingExtractor(schema(int_schema('a'), datetime_schema('b')));

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('due to data type mismatch - an explicit cast is required');

        try {
            df()
                ->read($extractor)
                ->filter(ref('a')->greaterThan(ref('b')))
                ->fetch();
        } finally {
            static::assertSame(0, $extractor->extractCalls);
        }
    }

    public function test_an_unresolved_column_is_refused_at_bind(): void
    {
        $extractor = new CountingExtractor(schema(int_schema('a'), int_schema('b')));

        $this->expectException(SchemaDefinitionNotFoundException::class);
        $this->expectExceptionMessage('Schema definition for entry "nope" not found');

        try {
            df()->read($extractor)->select('nope')->fetch();
        } finally {
            static::assertSame(0, $extractor->extractCalls);
        }
    }

    public function test_both_orderings_refuse_the_undeclared_path_segment(): void
    {
        $refusals = [];

        foreach (['get-then-select', 'select-then-get'] as $order) {
            $df = df()->read(from_array([['s' => ['a' => 1]]]));

            try {
                if ($order === 'get-then-select') {
                    $df
                        ->withEntry('g', array_get(ref('s'), 'typo'))
                        ->select('nope')
                        ->schema();
                } else {
                    $df
                        ->select('nope')
                        ->withEntry('g', array_get(ref('s'), 'typo'))
                        ->schema();
                }
            } catch (Throwable $e) {
                $refusals[$order] = $e::class;
            }
        }

        // an array_get() refusal is a build error, not a licence to skip the bind - whichever step
        // comes first is the one that names the problem, so the classes differ by ordering
        static::assertSame(
            [
                'get-then-select' => SchemaNotDerivableException::class,
                'select-then-get' => SchemaDefinitionNotFoundException::class,
            ],
            $refusals,
        );
    }

    public function test_a_discovering_pivot_over_a_non_repeatable_source_is_refused(): void
    {
        $this->expectException(SchemaNotDerivableException::class);
        $this->expectExceptionMessage('cannot read its dataset twice');

        df()
            ->read(from_data_frame(df()->read(from_array([
                ['product' => 'Banana', 'country' => 'USA', 'amount' => 1000],
            ]))))
            ->groupBy([ref('product')])
            ->pivot(ref('country'), discover_pivot_values());
    }

    public function test_discovered_pivot_values_scan_the_source_once_at_build(): void
    {
        $extractor = new CountingExtractor(
            $input = schema(str_schema('product'), str_schema('country'), int_schema('amount')),
            rows(
                $input,
                row(['product' => 'Banana', 'country' => 'USA', 'amount' => 1000]),
                row(['product' => 'Banana', 'country' => 'China', 'amount' => 400]),
            ),
        );

        $schema = df()
            ->read($extractor)
            ->groupBy([ref('product')])
            ->pivot(ref('country'), discover_pivot_values())
            ->aggregate(sum(ref('amount')))
            ->schema();

        static::assertSame(['product', 'China', 'USA'], $schema->references()->names());
        static::assertSame(1, $extractor->extractCalls);
    }

    public function test_pivot_declares_its_columns_at_bind(): void
    {
        $extractor = new CountingExtractor(schema(str_schema('product'), str_schema('country'), int_schema('amount')));

        $schema = df()
            ->read($extractor)
            ->groupBy([ref('product')])
            ->pivot(ref('country'), pivot_values('USA', 'China'))
            ->aggregate(sum(ref('amount')))
            ->schema();

        static::assertSame(['product', 'USA', 'China'], $schema->references()->names());
        static::assertSame(0, $extractor->extractCalls);
    }

    public function test_printing_the_schema_reads_no_row(): void
    {
        $extractor = new CountingExtractor(schema(int_schema('a')));

        ob_start();
        df()->read($extractor)->printSchema();
        $output = ob_get_clean() ?: '';

        static::assertStringContainsString('|-- a: integer', $output);
        static::assertSame(0, $extractor->extractCalls);
    }

    public function test_unpack_declares_its_columns_at_bind(): void
    {
        $extractor = new CountingExtractor(schema(int_schema('id'), str_schema('payload')));

        $schema = df()
            ->read($extractor)
            ->withEntry('p', ref('payload')->unpack(schema(str_schema('colour'), int_schema('size'))))
            ->schema();

        static::assertSame(['id', 'payload', 'p.colour', 'p.size'], $schema->references()->names());
        static::assertTrue($schema->get('p.colour')->isNullable());
        static::assertSame(0, $extractor->extractCalls);
    }

    /**
     * @param callable(): DataFrame $plan
     */
    #[DataProvider('plans')]
    public function test_the_declared_schema_is_the_produced_schema(callable $plan): void
    {
        $df = $plan();

        static::assertEquals($df->schema(), $df->fetch()->schema());
    }

    public function test_the_declared_schema_is_the_produced_schema_across_a_chain_of_steps(): void
    {
        $df = df()
            ->read(from_array([
                ['id' => 1, 'name' => 'a', 'when' => new DateTimeImmutable('2024-01-01')],
                ['id' => 2, 'name' => 'b', 'when' => new DateTimeImmutable('2024-01-02')],
            ]))
            ->withEntry('upper', ref('name')->upper())
            ->drop('when')
            ->select('id', 'upper');

        static::assertEquals($df->schema(), $df->fetch()->schema());
    }

    /**
     * The bucketing pair's metadata hop carries {id, totalRows} between the producer and its
     * consumer, and the plan declares the pre-bucketing schema. The invariant holds over the pair
     * because the hop never leaves it - so the pair's output is what has to be asserted.
     */
    public function test_the_bucketing_pair_declares_its_own_output_not_the_metadata_hop(): void
    {
        $df = df()->read(from_array(self::ROWS))->repartition(ref('group'));

        static::assertSame(['id', 'group', 'amount', 'name', 'payload'], $df->schema()->references()->names());
        static::assertEquals($df->schema(), $df->fetch()->schema());
    }

    public function test_the_plan_answers_a_row_less_source_with_its_declared_columns(): void
    {
        static::assertSame(
            ['a', 'b'],
            df()
                ->read(from_rows(rows(schema(int_schema('a'), int_schema('b')))))
                ->withEntry('b', ref('a')->plus(lit(1)))
                ->fetch()
                ->schema()
                ->references()
                ->names(),
        );
    }
}
