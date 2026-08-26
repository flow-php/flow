<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON\Tests\Integration\JSONMachine;

use Flow\ETL\Config;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\Adapter\JSON\from_json;
use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Filesystem\DSL\path_real;
use function usort;

final class JsonHydratorParityTest extends FlowTestCase
{
    public function test_fast_path_hydrates_typed_columns_with_missing_column_null_and_input_uri(): void
    {
        $extractor = from_json(
            $path = path_real(__DIR__ . '/../../Fixtures/parity_people.json'),
            schema: schema(int_schema('id'), str_schema('name', nullable: true), bool_schema('active')),
        )->withMetadataColumns(true);

        $actual = [];

        foreach ($extractor->extract(flow_context(Config::builder()->build())) as $rows) {
            foreach ($rows as $row) {
                $actual[] = $row->toArray();
            }
        }

        static::assertSame(
            [
                ['id' => 1, 'name' => 'Alice', 'active' => true, '_input_file_uri' => $path->uri()],
                ['id' => 2, 'name' => null, 'active' => false, '_input_file_uri' => $path->uri()],
            ],
            $actual,
        );
    }

    public function test_fast_path_reads_partitioned_files_with_partition_value_from_path(): void
    {
        $extractor = from_json(__DIR__ . '/../../Fixtures/parity_partitioned/group=*/data.json', schema: schema(
            int_schema('group'),
            int_schema('id'),
            str_schema('value'),
        ));

        $actual = [];

        foreach ($extractor->extract(flow_context(Config::builder()->build())) as $rows) {
            foreach ($rows as $row) {
                $actual[] = $row->toArray();
            }
        }

        usort($actual, static fn(array $a, array $b): int => (int) $a['id'] <=> (int) $b['id']);

        static::assertSame(
            [
                ['group' => 1, 'id' => 1, 'value' => 'a'],
                ['group' => 2, 'id' => 2, 'value' => 'b'],
            ],
            $actual,
        );
    }

    public function test_appends_partition_columns_absent_from_the_schema(): void
    {
        $extractor = from_json(__DIR__ . '/../../Fixtures/parity_partitioned/group=*/data.json', schema: schema(
            int_schema('id'),
            str_schema('value'),
        ));

        $actual = [];

        foreach ($extractor->extract(flow_context(Config::builder()->build())) as $rows) {
            foreach ($rows as $row) {
                $actual[] = $row->toArray();
            }
        }

        usort($actual, static fn(array $a, array $b): int => (int) $a['id'] <=> (int) $b['id']);

        static::assertSame(
            [
                ['id' => 1, 'value' => 'a', 'group' => '1'],
                ['id' => 2, 'value' => 'b', 'group' => '2'],
            ],
            $actual,
        );
    }
}
