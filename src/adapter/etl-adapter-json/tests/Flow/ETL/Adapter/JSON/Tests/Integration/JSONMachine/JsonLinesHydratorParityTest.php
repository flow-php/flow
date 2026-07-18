<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON\Tests\Integration\JSONMachine;

use Flow\ETL\Config;
use Flow\ETL\Row\AdaptiveRowHydrator;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\Adapter\JSON\from_json_lines;
use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Filesystem\DSL\path_real;

final class JsonLinesHydratorParityTest extends FlowTestCase
{
    public function test_hydrates_typed_columns_with_missing_column_null(): void
    {
        $extractor = from_json_lines(path_real(__DIR__ . '/../../Fixtures/parity_people.jsonl'))
            ->withSchema(schema(int_schema('id'), str_schema('name', nullable: true), bool_schema('active')));

        $actual = [];

        foreach ($extractor->extract(flow_context(Config::builder()->build())) as $rows) {
            foreach ($rows as $row) {
                $actual[] = $row->toArray();
            }
        }

        static::assertSame(
            [
                ['id' => 1, 'name' => 'Alice', 'active' => true],
                ['id' => 2, 'name' => null, 'active' => false],
            ],
            $actual,
        );
    }

    public function test_honours_a_hydrator_configured_on_the_context(): void
    {
        $extractor = from_json_lines(path_real(__DIR__ . '/../../Fixtures/parity_people.jsonl'))
            ->withSchema(schema(int_schema('id'), str_schema('name', nullable: true), bool_schema('active')));

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
                ['id' => 1, 'name' => 'Alice', 'active' => true],
                ['id' => 2, 'name' => null, 'active' => false],
            ],
            $actual,
        );
    }
}
