<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Text\Tests\Integration;

use Flow\ETL\Tests\FlowTestCase;
use Flow\Filesystem\Tests\OperatingSystem;

use function file_exists;
use function file_get_contents;
use function Flow\ETL\Adapter\Text\from_text;
use function Flow\ETL\Adapter\Text\to_text;
use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\from_sequence_number;
use function Flow\ETL\DSL\generate_random_string;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\overwrite;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\select;
use function Flow\ETL\DSL\string_entry;
use function Flow\ETL\DSL\to_transformation;
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

    public function test_transformation_loader_writes_all_batches_to_text(): void
    {
        data_frame()
            ->read(from_sequence_number('id', 1, 12))
            ->withEntry('name', lit('dropped by the transformation'))
            ->batchSize(4)
            ->write(to_transformation(
                select('id'),
                to_text($path = __DIR__ . '/var/test_transformation_loader.txt')->saveMode(overwrite()),
            ))
            ->run();

        $content = file_get_contents($path);

        if ($content === false) {
            static::fail('Failed to read file content');
        }

        static::assertStringNotContainsString('dropped by the transformation', $content);
        static::assertCount(12, data_frame()->read(from_text($path))->fetch());

        if (file_exists($path)) {
            unlink($path);
        }
    }
}
