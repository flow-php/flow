<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Text\Tests\Integration;

use Flow\ETL\DSL\Text;
use Flow\ETL\Flow;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\Serializer\CompressingSerializer;
use PHPUnit\Framework\TestCase;

final class TextLoaderTest extends TestCase
{
    public function test_loading_text_files_with_safe_mode() : void
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
            ->load(Text::to($path, true))
            ->run();

        $files = \array_values(\array_diff(\scandir($path), ['..', '.']));

        $this->assertStringContainsString(
            <<<'TEXT'
Norbert
Tomek
Dawid
TEXT,
            \file_get_contents($path . DIRECTORY_SEPARATOR . $files[0])
        );

        if (\file_exists($path . DIRECTORY_SEPARATOR . $files[0])) {
            \unlink($path . DIRECTORY_SEPARATOR . $files[0]);
        }
    }

    public function test_loading_text_files_without_safe_mode() : void
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
            ->load(Text::to($path))
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

    public function test_loading_text_files_without_safe_mode_and_with_serialization() : void
    {
        $path = \sys_get_temp_dir() . '/' . \uniqid('flow_php_etl_csv_loader', true) . '.csv';

        $serializer = new CompressingSerializer();

        (new Flow())
            ->process(
                new Rows(
                    Row::create(new Row\Entry\StringEntry('name', 'Norbert')),
                    Row::create(new Row\Entry\StringEntry('name', 'Tomek')),
                    Row::create(new Row\Entry\StringEntry('name', 'Dawid')),
                )
            )
            ->load($serializer->unserialize($serializer->serialize(Text::to($path))))
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
}
