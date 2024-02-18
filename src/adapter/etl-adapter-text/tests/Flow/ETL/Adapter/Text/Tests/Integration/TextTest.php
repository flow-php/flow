<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Text\Tests\Integration;

use function Flow\ETL\Adapter\Text\to_text;
use Flow\ETL\Filesystem\Path;
use Flow\ETL\Flow;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use PHPUnit\Framework\TestCase;

final class TextTest extends TestCase
{
    public function test_loading_text_files() : void
    {
        $path = \sys_get_temp_dir() . '/' . \uniqid('flow_php_etl_csv_loader', true) . '.csv';

        (new Flow())
            ->process(
                new Rows(
                    Row::create(new Row\Entry\StringEntry('name', 'Norbert')),
                    Row::create(new Row\Entry\StringEntry('name', 'Tomek')),
                    Row::create(new Row\Entry\StringEntry('name', 'Dawid')),
                )
            )
            ->write(to_text($path))
            ->run();

        $this->assertStringContainsString(
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

    public function test_using_pattern_path() : void
    {
        $this->expectExceptionMessage("TextLoader path can't be pattern, given: /path/*/pattern.csv");

        to_text(new Path('/path/*/pattern.csv'));
    }
}
