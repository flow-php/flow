<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Text\Tests\Integration;

use function Flow\ETL\Adapter\Text\to_text;
use Flow\ETL\{Flow, Row, Rows};
use Flow\Filesystem\Path;
use PHPUnit\Framework\TestCase;

final class TextTest extends TestCase
{
    public function test_loading_text_files() : void
    {
        $path = \sys_get_temp_dir() . '/flow_php_etl_csv_loader' . bin2hex(random_bytes(16)) . '.csv';

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

    public function test_using_pattern_path() : void
    {
        $this->expectExceptionMessage("TextLoader path can't be pattern, given: /path/*/pattern.csv");

        to_text(new Path('/path/*/pattern.csv'));
    }
}
