<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Text\Tests\Integration;

use function Flow\ETL\Adapter\Text\to_text;
use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\{generate_random_string, string_entry};
use function Flow\ETL\DSL\{row, rows};
use Flow\ETL\{Tests\FlowTestCase};

final class TextTest extends FlowTestCase
{
    public function test_loading_text_files() : void
    {
        $path = __DIR__ . '/var/flow_php_etl_csv_loader' . generate_random_string() . '.csv';

        (data_frame())
            ->process(
                rows(row(string_entry('name', 'Norbert')), row(string_entry('name', 'Tomek')), row(string_entry('name', 'Dawid')))
            )
            ->write(to_text($path))
            ->run();

        self::assertStringContainsString(
            <<<'TEXT'
Norbert
Tomek
Dawid
TEXT,
            \file_get_contents($path)
        );

        if (\file_exists($path)) {
            \unlink($path);
        }
    }
}
