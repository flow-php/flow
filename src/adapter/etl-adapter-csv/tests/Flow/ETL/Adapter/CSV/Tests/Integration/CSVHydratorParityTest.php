<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV\Tests\Integration;

use Flow\ETL\Config;
use Flow\ETL\Row\AdaptiveRowHydrator;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\Adapter\CSV\from_csv;
use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Filesystem\DSL\path_real;
use function usort;

final class CSVHydratorParityTest extends FlowTestCase
{
    public function test_hydrates_typed_columns_with_empty_to_null_and_input_uri(): void
    {
        $extractor = from_csv($path = path_real(__DIR__ . '/../Fixtures/file_with_empty_columns.csv'))
            ->withSchema(schema(
                int_schema('id', nullable: true),
                str_schema('name', nullable: true),
                bool_schema('active'),
            ))
            ->withMetadataColumns(true);

        $actual = [];

        foreach ($extractor->extract(flow_context(Config::builder()->build())) as $rows) {
            foreach ($rows as $row) {
                $actual[] = $row->toArray();
            }
        }

        static::assertSame(
            [
                ['id' => null, 'name' => null, 'active' => false, '_input_file_uri' => $path->uri()],
                ['id' => 1, 'name' => 'Norbert', 'active' => null, '_input_file_uri' => $path->uri()],
            ],
            $actual,
        );
    }

    public function test_honours_a_hydrator_configured_on_the_context(): void
    {
        $extractor = from_csv(path_real(__DIR__ . '/../Fixtures/file_with_empty_columns.csv'))
            ->withSchema(schema(
                int_schema('id', nullable: true),
                str_schema('name', nullable: true),
                bool_schema('active'),
            ));

        $actual = [];

        foreach ($extractor->extract(
            flow_context(Config::builder()->hydrator(new AdaptiveRowHydrator())->build()),
        ) as $rows) {
            foreach ($rows as $row) {
                $actual[] = $row->toArray();
            }
        }

        static::assertSame(
            [
                ['id' => null, 'name' => null, 'active' => false],
                ['id' => 1, 'name' => 'Norbert', 'active' => null],
            ],
            $actual,
        );
    }

    public function test_reads_partitioned_files_with_schema_typed_partition_values(): void
    {
        $extractor = from_csv(__DIR__ . '/../Fixtures/partitioned/group=*/*.csv')->withSchema(schema(
            int_schema('group'),
            int_schema('id'),
            str_schema('value'),
        ));

        $actual = [];
        $partitions = [];

        foreach ($extractor->extract(flow_context(Config::builder()->build())) as $rows) {
            foreach ($rows as $row) {
                $actual[] = $row->toArray();
            }

            foreach ($rows->partitions() as $partition) {
                $partitions[$partition->name] = true;
            }
        }

        usort($actual, static fn(array $a, array $b): int => (int) $a['id'] <=> (int) $b['id']);

        static::assertSame(
            [
                ['group' => 1, 'id' => 1, 'value' => 'a'],
                ['group' => 1, 'id' => 2, 'value' => 'b'],
                ['group' => 1, 'id' => 3, 'value' => 'c'],
                ['group' => 1, 'id' => 4, 'value' => 'd'],
                ['group' => 2, 'id' => 5, 'value' => 'e'],
                ['group' => 2, 'id' => 6, 'value' => 'f'],
                ['group' => 2, 'id' => 7, 'value' => 'g'],
                ['group' => 2, 'id' => 8, 'value' => 'h'],
            ],
            $actual,
        );
        static::assertArrayHasKey('group', $partitions);
    }

    public function test_appends_partition_columns_absent_from_the_schema(): void
    {
        $extractor = from_csv(__DIR__ . '/../Fixtures/partitioned/group=1/file_01.csv')->withSchema(schema(
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
                ['id' => 2, 'value' => 'b', 'group' => '1'],
            ],
            $actual,
        );
    }
}
