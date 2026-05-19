<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Text\Tests\Integration;

use Flow\ETL\Tests\FlowTestCase;
use Flow\Filesystem\Tests\OperatingSystem;

use function file_exists;
use function file_get_contents;
use function Flow\ETL\Adapter\Text\to_text;
use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\generate_random_string;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\string_entry;
use function unlink;

final class TextTest extends FlowTestCase
{
    use OperatingSystem;

    public function test_loading_text_files(): void
    {
        if ($this->isWindows()) {
            static::markTestSkipped('This test is failing on windows due to different new line characters.');
        }

        $path = __DIR__ . '/var/flow_php_etl_csv_loader' . generate_random_string() . '.csv';

        data_frame()
            ->process(rows(
                row(string_entry('name', 'Norbert')),
                row(string_entry('name', 'Tomek')),
                row(string_entry('name', 'Dawid')),
            ))
            ->write(to_text($path))
            ->run();

        $content = file_get_contents($path);
        static::assertNotFalse($content);
        static::assertStringContainsString(<<<'TEXT'
            Norbert
            Tomek
            Dawid
            TEXT, $content);

        if (file_exists($path)) {
            unlink($path);
        }
    }
}
