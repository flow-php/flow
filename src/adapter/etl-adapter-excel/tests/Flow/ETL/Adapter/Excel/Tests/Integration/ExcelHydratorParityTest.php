<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Excel\Tests\Integration;

use Flow\ETL\Config;
use Flow\ETL\Row\AdaptiveRowHydrator;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\Adapter\Excel\DSL\from_excel;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\string_schema;
use function Flow\Filesystem\DSL\path_real;
use function Flow\Types\DSL\type_integer;

final class ExcelHydratorParityTest extends FlowTestCase
{
    public function test_reads_typed_columns_with_a_schema(): void
    {
        $extractor = from_excel(path_real(__DIR__ . '/../Fixtures/fixture.xlsx'))
            ->withSchema(schema(int_schema('id'), string_schema('name'), string_schema('email', nullable: true)));

        $count = 0;

        foreach ($extractor->extract(flow_context(Config::builder()->build())) as $rows) {
            static::assertEquals(type_integer(), $rows->schema()->get('id')->type());

            $count += $rows->count();
        }

        static::assertSame(10, $count);
    }

    public function test_appends_input_file_uri_when_metadata_columns_are_enabled(): void
    {
        $extractor = from_excel($path = path_real(__DIR__ . '/../Fixtures/fixture.xlsx'))
            ->withSchema(schema(int_schema('id'), string_schema('name'), string_schema('email', nullable: true)))
            ->withMetadataColumns(true);

        foreach ($extractor->extract(flow_context(Config::builder()->build())) as $rows) {
            foreach ($rows as $row) {
                static::assertTrue($row->has('_input_file_uri'));
                static::assertSame($path->uri(), $row->get('_input_file_uri'));
            }
        }
    }

    public function test_honours_a_hydrator_configured_on_the_context(): void
    {
        $extractor = from_excel(path_real(__DIR__ . '/../Fixtures/fixture.xlsx'));

        $count = 0;

        foreach ($extractor->extract(
            flow_context(Config::builder()->hydrator(new AdaptiveRowHydrator())->build()),
        ) as $rows) {
            $count += $rows->count();
        }

        static::assertSame(10, $count);
    }
}
