<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Text\Tests\Integration;

use Flow\ETL\DSL\Text;
use Flow\ETL\Filesystem\Path;
use Flow\ETL\Flow;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Test\FilesystemTestHelper;
use Flow\Serializer\CompressingSerializer;
use PHPUnit\Framework\TestCase;

final class TextLoaderTest extends TestCase
{
    use FilesystemTestHelper;

    public function test_loading_text_files_with_append_safe() : void
    {
        $path = $this->createTemporaryFile('flow_php_etl_csv_loader', '.csv');

        (new Flow())
            ->process(
                new Rows(
                    Row::create(new Row\Entry\StringEntry('name', 'Norbert')),
                    Row::create(new Row\Entry\StringEntry('name', 'Tomek')),
                    Row::create(new Row\Entry\StringEntry('name', 'Dawid')),
                )
            )
            ->load(Text::to($path))
            ->appendSafe()
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

        $this->removeFile($path . DIRECTORY_SEPARATOR . $files[0]);
    }

    public function test_loading_text_files_without_safe_mode() : void
    {
        $path = $this->createTemporaryFile('flow_php_etl_csv_loader', '.csv');

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

        $this->removeFile($path);
    }

    public function test_loading_text_files_without_safe_mode_and_with_serialization() : void
    {
        $path = $this->createTemporaryFile('flow_php_etl_csv_loader', '.csv');

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

        $this->removeFile($path);
    }

    public function test_using_pattern_path() : void
    {
        $this->expectExceptionMessage("TextLoader path can't be pattern, given: /path/*/pattern.csv");

        Text::to(new Path('/path/*/pattern.csv'));
    }
}
