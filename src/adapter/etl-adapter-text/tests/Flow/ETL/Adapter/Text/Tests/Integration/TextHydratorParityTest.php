<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Text\Tests\Integration;

use Flow\ETL\Config;
use Flow\ETL\Row\AdaptiveRowHydrator;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\Adapter\Text\from_text;
use function Flow\ETL\DSL\flow_context;
use function Flow\Filesystem\DSL\path_real;
use function Flow\Types\DSL\type_string;

final class TextHydratorParityTest extends FlowTestCase
{
    public function test_reads_each_line_into_a_string_text_column(): void
    {
        $extractor = from_text(path_real(__DIR__ . '/../Fixtures/parity_lines.txt'));

        $actual = [];

        foreach ($extractor->extract(flow_context(Config::builder()->build())) as $rows) {
            static::assertEquals(type_string(), $rows->schema()->get('text')->type());

            foreach ($rows as $row) {
                $actual[] = $row->toArray();
            }
        }

        static::assertSame([['text' => 'alpha'], ['text' => 'beta'], ['text' => 'gamma']], $actual);
    }

    public function test_appends_input_file_uri_when_metadata_columns_are_enabled(): void
    {
        $extractor = from_text($path = path_real(__DIR__ . '/../Fixtures/parity_lines.txt'))->withMetadataColumns(true);

        $actual = [];

        foreach ($extractor->extract(flow_context(Config::builder()->build())) as $rows) {
            foreach ($rows as $row) {
                $actual[] = $row->toArray();
            }
        }

        static::assertSame(
            [
                ['text' => 'alpha', '_input_file_uri' => $path->uri()],
                ['text' => 'beta', '_input_file_uri' => $path->uri()],
                ['text' => 'gamma', '_input_file_uri' => $path->uri()],
            ],
            $actual,
        );
    }

    public function test_honours_a_hydrator_configured_on_the_context(): void
    {
        $extractor = from_text(path_real(__DIR__ . '/../Fixtures/parity_lines.txt'));

        $actual = [];

        foreach ($extractor->extract(
            flow_context(Config::builder()->hydrator(new AdaptiveRowHydrator())->build()),
        ) as $rows) {
            foreach ($rows as $row) {
                $actual[] = $row->toArray();
            }
        }

        static::assertSame([['text' => 'alpha'], ['text' => 'beta'], ['text' => 'gamma']], $actual);
    }
}
