<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Foundation\Exception;

use DateTimeImmutable;
use Flow\ETL\Foundation\Exception\CorruptedRecordException;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Foundation\Exception\RuntimeException;
use Flow\ETL\Tests\FlowTestCase;

use function str_repeat;

final class CorruptedRecordExceptionTest extends FlowTestCase
{
    public function test_the_message_names_the_record_where_it_is_why_and_the_way_out(): void
    {
        static::assertSame(
            'Corrupted record at offset 12, line 3, of file:///a.csv: 3 fields, the header has 2: "1,a,x". Route '
            . 'corrupted records to a loader with ->withCorruptedRecords($loader).',
            (new CorruptedRecordException('file:///a.csv', 12, 3, '3 fields, the header has 2', '1,a,x'))->getMessage(),
        );
    }

    public function test_a_record_without_a_line_or_a_uri_and_too_long_to_show_whole(): void
    {
        static::assertSame(
            'Corrupted record at offset 4 of the source: reason: "'
            . str_repeat('ü', 120)
            . '…". Route corrupted '
            . 'records to a loader with ->withCorruptedRecords($loader).',
            (new CorruptedRecordException(null, 4, null, 'reason', str_repeat('ü', 121)))->getMessage(),
        );
    }

    public function test_cells_are_kept_as_strings_and_shown_as_a_json_list_without_the_source_text(): void
    {
        $corrupted = CorruptedRecordException::fromCells('file:///a.xlsx', 5, null, 'reason', [
            2 => 'a/ü',
            3 => null,
            4 => new DateTimeImmutable('2026-10-31 12:00:00+00:00'),
            5 => 1.5,
            6 => false,
            7 => ['x'],
        ]);

        static::assertSame(['a/ü', '', '2026-10-31T12:00:00+00:00', '1.5', '', '["x"]'], $corrupted->fields);
        static::assertSame('["a/ü","","2026-10-31T12:00:00+00:00","1.5","","[\\"x\\"]"]', $corrupted->record);
    }

    public function test_cells_keep_the_source_text_when_the_source_holds_one(): void
    {
        $corrupted = CorruptedRecordException::fromCells('file:///a.csv', 0, 1, 'reason', ['1', 'a'], '1,a');

        static::assertSame(['1,a', ['1', 'a']], [$corrupted->record, $corrupted->fields]);
    }

    public function test_values_are_shown_as_a_json_object_without_the_source_text(): void
    {
        $corrupted = CorruptedRecordException::fromValues('file:///a.json', 4, null, 'reason', [
            'id' => 'n/a',
            'at' => 'ü/x',
        ]);

        static::assertSame(['{"id":"n/a","at":"ü/x"}', null], [$corrupted->record, $corrupted->fields]);
    }

    public function test_values_keep_the_source_text_when_the_source_holds_one(): void
    {
        static::assertSame(
            '{"id": "x"}',
            CorruptedRecordException::fromValues(
                'file:///a.jsonl',
                0,
                1,
                'reason',
                ['id' => 'x'],
                '{"id": "x"}',
            )->record,
        );
    }

    public function test_from_record_is_the_inverse_of_to_record_and_keeps_the_previous(): void
    {
        $previous = new RuntimeException('from a task');
        $corrupted = CorruptedRecordException::fromCells('file:///a.csv', 12, 3, 'reason', ['1', 'a', 'x'], '1,a,x');

        $rebuilt = CorruptedRecordException::fromRecord($corrupted->toRecord(), $previous);

        static::assertSame($corrupted->toRecord(), $rebuilt->toRecord());
        static::assertSame($corrupted->getMessage(), $rebuilt->getMessage());
        static::assertSame($previous, $rebuilt->getPrevious());
    }

    public function test_from_record_round_trips_a_keyed_record_without_a_uri_or_a_line(): void
    {
        $corrupted = CorruptedRecordException::fromValues(null, 4, null, 'reason', ['id' => 'x']);

        static::assertSame(
            $corrupted->toRecord(),
            CorruptedRecordException::fromRecord($corrupted->toRecord())->toRecord(),
        );
        static::assertNull(CorruptedRecordException::fromRecord($corrupted->toRecord())->getPrevious());
    }

    public function test_from_record_refuses_a_record_of_another_shape(): void
    {
        $this->expectException(InvalidArgumentException::class);

        CorruptedRecordException::fromRecord(['uri' => null, 'offset' => '4']);
    }
}
