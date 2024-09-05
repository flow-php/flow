<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Text\Tests\Integration;

use function Flow\ETL\Adapter\Text\to_text;
use Flow\ETL\{Flow, Row, Rows};
use PHPUnit\Framework\TestCase;

final class TextTest extends TestCase
{
    public function test_loading_text_files() : void
    {
        $path = __DIR__ . '/var/flow_php_etl_csv_loader' . \Flow\ETL\DSL\generate_random_string() . '.csv';

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
}
