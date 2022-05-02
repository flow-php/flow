<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV\Tests\Integration\League;

use Flow\ETL\Adapter\CSV\League\CSVLoader;
use Flow\ETL\DSL\CSV;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\Serializer\CompressingSerializer;
use Flow\Serializer\NativePHPSerializer;
use PHPUnit\Framework\TestCase;

final class CSVLoaderTest extends TestCase
{
    public function test_loading_csv_files_with_safe_mode() : void
    {
        $path = \sys_get_temp_dir() . '/' . \uniqid('flow_php_etl_csv_loader', true) . '.csv';

        $loader = CSV::to_directory($path);

        $loader->load(new Rows(
            Row::create(new Row\Entry\IntegerEntry('id', 1), new Row\Entry\StringEntry('name', 'Norbert')),
            Row::create(new Row\Entry\IntegerEntry('id', 2), new Row\Entry\StringEntry('name', 'Tomek')),
        ));
        $loader->load(new Rows(
            Row::create(new Row\Entry\IntegerEntry('id', 3), new Row\Entry\StringEntry('name', 'Dawid')),
        ));

        $files = \array_values(\array_diff(\scandir($path), ['..', '.']));

        $this->assertStringContainsString(
            <<<'CSV'
id,name
1,Norbert
2,Tomek
3,Dawid
CSV,
            \file_get_contents($path . DIRECTORY_SEPARATOR . $files[0])
        );

        if (\file_exists($path . DIRECTORY_SEPARATOR . $files[0])) {
            \unlink($path . DIRECTORY_SEPARATOR . $files[0]);
        }
    }

    public function test_loading_csv_files_without_safe_mode() : void
    {
        $path = \sys_get_temp_dir() . '/' . \uniqid('flow_php_etl_csv_loader', true) . '.csv';

        $loader = CSV::to_file($path, 'w+', $withHeader = true);

        $loader->load(new Rows(
            Row::create(new Row\Entry\IntegerEntry('id', 1), new Row\Entry\StringEntry('name', 'Norbert')),
            Row::create(new Row\Entry\IntegerEntry('id', 2), new Row\Entry\StringEntry('name', 'Tomek')),
        ));
        $loader->load(new Rows(
            Row::create(new Row\Entry\IntegerEntry('id', 3), new Row\Entry\StringEntry('name', 'Dawid')),
        ));

        $this->assertStringContainsString(
            <<<'CSV'
id,name
1,Norbert
2,Tomek
3,Dawid
CSV,
            \file_get_contents($path)
        );

        if (\file_exists($path)) {
            \unlink($path);
        }
    }

    public function test_loading_csv_files_without_safe_mode_and_with_serialization() : void
    {
        $path = \sys_get_temp_dir() . '/' . \uniqid('flow_php_etl_csv_loader', true) . '.csv';

        $serializer = new CompressingSerializer(new NativePHPSerializer());

        $loader = $serializer->unserialize($serializer->serialize(new CSVLoader($path, 'w+', $withHeader = true, $safeMode = false)));

        $loader->load(new Rows(
            Row::create(new Row\Entry\IntegerEntry('id', 1), new Row\Entry\StringEntry('name', 'Norbert')),
            Row::create(new Row\Entry\IntegerEntry('id', 2), new Row\Entry\StringEntry('name', 'Tomek')),
        ));
        $loader->load(new Rows(
            Row::create(new Row\Entry\IntegerEntry('id', 3), new Row\Entry\StringEntry('name', 'Dawid')),
        ));

        $this->assertStringContainsString(
            <<<'CSV'
id,name
1,Norbert
2,Tomek
3,Dawid
CSV,
            \file_get_contents($path)
        );

        if (\file_exists($path)) {
            \unlink($path);
        }
    }

    public function test_loading_csv_files_with_empty_row() : void
    {
        $path = \sys_get_temp_dir() . '/' . \uniqid('flow_php_etl_csv_loader', true) . '.csv';

        $loader = CSV::to_file($path, 'w+', $withHeader = true);

        $loader->load(new Rows(
        ));

        $loader->load(new Rows(
            Row::create(new Row\Entry\IntegerEntry('id', 1), new Row\Entry\StringEntry('name', 'Norbert')),
        ));

        $this->assertStringContainsString(
            <<<'CSV'
id,name
1,Norbert
CSV,
            \file_get_contents($path)
        );

        if (\file_exists($path)) {
            \unlink($path);
        }
    }
}
