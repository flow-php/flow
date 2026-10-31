<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Values\Rows;

use DateTimeImmutable;
use Flow\ETL\Foundation\Exception\CorruptedRecordException;
use Flow\ETL\Foundation\Exception\InferredSchemaException;
use Flow\ETL\Foundation\Exception\SchemaMismatchException;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\Column\PhpBackend;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Rows\RecordBatch;

use function array_map;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function iterator_to_array;

final class RecordBatchTest extends FlowTestCase
{
    public function test_a_batch_is_handed_out_once_it_is_full_and_the_rest_on_flush(): void
    {
        $batch = new RecordBatch(schema(int_schema('id')), new PhpBackend(), 2);

        static::assertSame([], $batch->add(['id' => 1], 1));
        $full = $batch->add(['id' => 2], 2);
        static::assertSame([], $batch->add(['id' => 3], 3));
        $rest = $batch->flush();

        static::assertSame(
            [[['id' => 1], ['id' => 2]]],
            array_map(static fn(CorruptedRecordException|Rows $rows): array => (
                $rows instanceof Rows ? $rows->toArray() : [$rows->getMessage()]
            ), $full),
        );
        static::assertSame(
            [[['id' => 3]]],
            array_map(static fn(CorruptedRecordException|Rows $rows): array => (
                $rows instanceof Rows ? $rows->toArray() : [$rows->getMessage()]
            ), $rest),
        );
        static::assertSame([], $batch->flush());
    }

    public function test_nothing_added_flushes_nothing(): void
    {
        static::assertSame([], (new RecordBatch(schema(int_schema('id')), new PhpBackend(), 1))->flush());
    }

    public function test_under_a_declared_schema_a_value_that_does_not_cast_is_a_corrupted_record(): void
    {
        $batch = new RecordBatch(schema(int_schema('id')), new PhpBackend(), 3, 'file:///orders.csv');
        $batch->add(['id' => '1'], 3, 2, '1', ['1']);
        $batch->add(['id' => 'x'], 5, 3, 'x', ['x']);

        [$corrupted, $rows] = $batch->add(['id' => '3'], 7, 4, '3', ['3']);

        static::assertInstanceOf(CorruptedRecordException::class, $corrupted);
        static::assertSame(['file:///orders.csv', 5, 3, 'x', ['x']], [
            $corrupted->uri,
            $corrupted->offset,
            $corrupted->lineNumber,
            $corrupted->record,
            $corrupted->fields,
        ]);
        static::assertStringStartsWith('column "id"', $corrupted->reason);
        static::assertInstanceOf(Rows::class, $rows);
        static::assertSame([['id' => 1], ['id' => 3]], $rows->toArray());
    }

    public function test_every_corrupted_record_of_a_batch_comes_before_its_rows(): void
    {
        $batch = new RecordBatch(schema(int_schema('id')), new PhpBackend(), 3);
        $batch->add(['id' => 'x'], 1);
        $batch->add(['id' => '2'], 2);
        $handed = $batch->add(['id' => 'y'], 3);

        static::assertSame(
            [1, 3, [['id' => 2]]],
            array_map(static fn(CorruptedRecordException|Rows $item): array|int => $item instanceof Rows
                ? $item->toArray()
                : $item->offset, $handed),
        );
    }

    public function test_every_refused_record_of_a_batch_is_handed_out_in_order_with_its_offset_and_the_rest_built(): void
    {
        $batch = new RecordBatch(schema(int_schema('id'), str_schema('name')), new PhpBackend(), 6, 'file:///a.csv');
        $batch->add(['id' => '1', 'name' => 'a'], 10, 2);
        $batch->add(['id' => '1'], 20, 3);
        $batch->add(['id' => 'x', 'name' => 'b'], 30, 4);
        $batch->add(['id' => '4', 'name' => 'c'], 40, 5);
        $batch->add(['id' => 'y'], 50, 6);

        static::assertSame(
            [
                [20, 3, 'column "name" declared by the schema is missing from the row'],
                [30, 4, 'column "id": could not convert \'x\' (string) to integer'],
                [50, 6, 'column "id": could not convert \'y\' (string) to integer'],
                [['id' => 1, 'name' => 'a'], ['id' => 4, 'name' => 'c'], ['id' => 6, 'name' => 'd']],
            ],
            array_map(
                static fn(CorruptedRecordException|Rows $item): array => (
                    $item instanceof Rows ? $item->toArray() : [$item->offset, $item->lineNumber, $item->reason]
                ),
                $batch->add(['id' => '6', 'name' => 'd'], 60, 7),
            ),
        );
    }

    public function test_a_batch_of_corrupted_records_only_hands_out_no_rows(): void
    {
        $batch = new RecordBatch(schema(int_schema('id')), new PhpBackend(), 2);
        $batch->add(['id' => 'x'], 1);

        static::assertCount(1, $batch->flush());
        static::assertSame([], $batch->flush());
    }

    public function test_a_record_without_source_text_is_its_json(): void
    {
        $batch = new RecordBatch(schema(int_schema('id'), str_schema('name')), new PhpBackend(), 1);

        [$corrupted] = $batch->add(['id' => 'x', 'name' => 'a/ü'], 2);

        static::assertInstanceOf(CorruptedRecordException::class, $corrupted);
        static::assertSame(['{"id":"x","name":"a/ü"}', null, null], [
            $corrupted->record,
            $corrupted->fields,
            $corrupted->uri,
        ]);
    }

    public function test_the_fields_of_a_grid_record_are_their_string_form(): void
    {
        $batch = new RecordBatch(
            schema(int_schema('id'), str_schema('at'), str_schema('a', true)),
            new PhpBackend(),
            1,
        );
        $at = new DateTimeImmutable('2026-10-31 12:00:00+00:00');

        [$corrupted] = $batch->add(['id' => 'x', 'at' => 'n', 'a' => null], 4, fields: ['x', $at, null, 1.5, true]);

        static::assertInstanceOf(CorruptedRecordException::class, $corrupted);
        static::assertSame(['x', '2026-10-31T12:00:00+00:00', '', '1.5', '1'], $corrupted->fields);
        static::assertSame('["x","2026-10-31T12:00:00+00:00","","1.5","1"]', $corrupted->record);
    }

    public function test_under_an_inferred_schema_a_refused_record_past_the_sample_is_a_schema_error(): void
    {
        $batch = new RecordBatch(schema(int_schema('id')), new PhpBackend(), 2, 'file:///orders.csv', 2);
        $batch->add(['id' => 1], 1);
        $batch->add(['id' => 2], 2);
        $batch->add(['id' => 3], 3);

        $this->expectException(InferredSchemaException::class);
        $this->expectExceptionMessage(
            'Record at offset 4 of file:///orders.csv does not fit the schema inferred from a sample of 2 rows',
        );

        $batch->add(['id' => 'x'], 4);
    }

    public function test_under_an_inferred_schema_without_a_uri_the_refusal_is_left_to_the_reader(): void
    {
        $batch = new RecordBatch(schema(int_schema('id')), new PhpBackend(), 2, sampleSize: 2);
        $batch->add(['id' => 1], 1);
        $batch->add(['id' => 2], 2);
        $batch->add(['id' => 3], 3);

        $this->expectException(SchemaMismatchException::class);
        $this->expectExceptionMessage('column "id" (row 3)');

        $batch->add(['id' => 'x'], 4);
    }

    public function test_under_an_inferred_schema_a_keyed_record_refuses_a_key_the_schema_lacks(): void
    {
        $batch = new RecordBatch(
            schema(int_schema('a')),
            new PhpBackend(),
            1,
            sampleSize: 1,
            known: schema(int_schema('a')),
        );

        $this->expectException(SchemaMismatchException::class);
        $this->expectExceptionMessage('column "c" (row 0) is not declared by the schema');

        $batch->add(['a' => 1, 'c' => 2], 1);
    }

    public function test_under_an_inferred_schema_a_keyed_record_keeps_a_key_the_known_schema_declares(): void
    {
        $batch = new RecordBatch(
            schema(int_schema('a')),
            new PhpBackend(),
            1,
            'file:///group=1/data.jsonl',
            1,
            schema(int_schema('a'), str_schema('group')),
        );

        [$rows] = $batch->add(['a' => 1, 'group' => '2'], 0, 1);

        static::assertInstanceOf(Rows::class, $rows);
        static::assertSame([['a' => 1]], $rows->toArray());
    }

    public function test_under_an_inferred_schema_a_keyed_record_key_past_the_sample_names_the_record(): void
    {
        $batch = new RecordBatch(
            schema(int_schema('a')),
            new PhpBackend(),
            1,
            'file:///b.jsonl',
            1000,
            schema(int_schema('a')),
        );

        $this->expectException(InferredSchemaException::class);
        $this->expectExceptionMessage(
            'Record at offset 23, line 2, of file:///b.jsonl does not fit the schema inferred from a sample of 1000 rows: '
            . 'column "c"',
        );

        $batch->add(['a' => 1, 'c' => 2], 23, 2);
    }

    public function test_under_an_inferred_schema_a_grid_record_keeps_a_partition_column_its_header_carries(): void
    {
        $batch = new RecordBatch(schema(int_schema('a')), new PhpBackend(), 1, 'file:///group=1/a.csv', 1);

        [$rows] = $batch->add(['a' => '1', 'group' => '2'], 4, 2, '1,2', ['1', '2']);

        static::assertInstanceOf(Rows::class, $rows);
        static::assertSame([['a' => 1]], $rows->toArray());
    }

    public function test_a_refused_record_takes_its_slot_and_leaves_in_source_order_with_the_cast_refusals(): void
    {
        $batch = new RecordBatch(schema(int_schema('id')), new PhpBackend(), 4);
        $batch->add(['id' => 'x'], 10);
        $batch->refuse(new CorruptedRecordException(null, 20, null, '3 fields, the header has 1', 'a,b,c'));
        $batch->add(['id' => 'y'], 30);

        static::assertSame(
            [10, 20, 30, [['id' => 4]]],
            array_map(
                static fn(CorruptedRecordException|Rows $item): array|int => $item instanceof Rows
                    ? $item->toArray()
                    : $item->offset,
                $batch->add(['id' => 4], 40),
            ),
        );
    }

    public function test_a_batch_of_refused_records_only_flushes_them(): void
    {
        $batch = new RecordBatch(schema(int_schema('id')), new PhpBackend(), 10);
        $refused = new CorruptedRecordException(null, 1, null, 'reason', 'x');
        $batch->refuse($refused);

        static::assertSame([$refused], $batch->flush());
        static::assertSame([], $batch->flush());
    }

    public function test_of_numbers_the_records_from_one_and_flushes_the_rest(): void
    {
        $batch = new RecordBatch(schema(int_schema('id')), new PhpBackend(), 2);

        static::assertSame(
            [2, [['id' => 1]], [['id' => 3], ['id' => 4]]],
            array_map(
                static fn(CorruptedRecordException|Rows $item): array|int => $item instanceof Rows
                    ? $item->toArray()
                    : $item->offset,
                iterator_to_array($batch->of([['id' => 1], ['id' => 'x'], ['id' => 3], ['id' => 4]]), false),
            ),
        );
    }
}
