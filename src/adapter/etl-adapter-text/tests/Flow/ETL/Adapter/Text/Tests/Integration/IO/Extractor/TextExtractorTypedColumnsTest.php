<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Text\Tests\Integration\IO\Extractor;

use Flow\ETL\IO\Extractor\Criteria;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\Adapter\Text\from_text;
use function Flow\ETL\DSL\config_builder;
use function Flow\ETL\DSL\flow_context;
use function Flow\Filesystem\DSL\path_real;
use function Flow\Types\DSL\type_string;

final class TextExtractorTypedColumnsTest extends FlowTestCase
{
    public function test_reads_each_line_into_a_string_text_column(): void
    {
        $extractor = from_text(path_real(__DIR__ . '/../../../Fixtures/parity_lines.txt'));

        $actual = [];

        foreach (FlowTestCase::extracted(
            $extractor,
            flow_context(config_builder()->build())->backend(),
            new Criteria(),
        ) as $rows) {
            static::assertEquals(type_string(), $rows->schema()->get('text')->type());

            foreach ($rows->toArray() as $row) {
                $actual[] = $row;
            }
        }

        static::assertSame([['text' => 'alpha'], ['text' => 'beta'], ['text' => 'gamma']], $actual);
    }

    public function test_appends_input_file_uri_when_metadata_columns_are_enabled(): void
    {
        $extractor = from_text($path = path_real(__DIR__ . '/../../../Fixtures/parity_lines.txt'))
            ->withMetadataColumns(true);

        $actual = [];

        foreach (FlowTestCase::extracted(
            $extractor,
            flow_context(config_builder()->build())->backend(),
            new Criteria(),
        ) as $rows) {
            foreach ($rows->toArray() as $row) {
                $actual[] = $row;
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
}
