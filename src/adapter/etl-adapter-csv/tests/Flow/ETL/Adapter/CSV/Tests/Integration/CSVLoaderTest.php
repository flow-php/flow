<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV\Tests\Integration;

use function Flow\ETL\Adapter\CSV\to_csv;
use function Flow\ETL\DSL\array_entry;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\overwrite;
use function Flow\ETL\DSL\str_entry;
use Flow\ETL\Filesystem\Path;
use Flow\ETL\Flow;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use PHPUnit\Framework\TestCase;

final class CSVLoaderTest extends TestCase
{
    public function test_loading_array_entry() : void
    {
        $path = \sys_get_temp_dir() . '/' . \uniqid('flow_php_etl_csv_loader', true) . '.csv';

        $this->expectExceptionMessage('Entry "data" is an list|array, please cast to string before writing to CSV. Easiest way to cast arrays to string is to use Transform::to_json transformer.');

        (new Flow())
            ->process(
                new Rows(
                    Row::create(int_entry('id', 1), array_entry('data', ['foo' => 'bar'])),
                )
            )
            ->write(to_csv($path))
            ->run();

        if (\file_exists($path)) {
            \unlink($path);
        }
    }

    public function test_loading_csv_files_with_append_safe() : void
    {
        $path = \sys_get_temp_dir() . '/' . \uniqid('flow_php_etl_csv_loader', true) . '.csv';

        (new Flow())
            ->process(
                new Rows(
                    Row::create(new Row\Entry\IntegerEntry('id', 1), new Row\Entry\StringEntry('name', 'Norbert')),
                    Row::create(new Row\Entry\IntegerEntry('id', 2), new Row\Entry\StringEntry('name', 'Tomek')),
                    Row::create(new Row\Entry\IntegerEntry('id', 3), new Row\Entry\StringEntry('name', 'Dawid')),
                )
            )
            ->appendSafe()
            ->load(to_csv($path))
            ->run();

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

    public function test_loading_csv_files_without_threadsafe() : void
    {
        $path = \sys_get_temp_dir() . '/' . \uniqid('flow_php_etl_csv_loader', true) . '.csv';

        (new Flow())
            ->process(
                new Rows(
                    Row::create(int_entry('id', 1), str_entry('name', 'Norbert')),
                    Row::create(int_entry('id', 2), str_entry('name', 'Tomek')),
                    Row::create(int_entry('id', 3), str_entry('name', 'Dawid')),
                )
            )
            ->load(to_csv($path))
            ->run();

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

    public function test_loading_csv_with_partitioning() : void
    {
        $path = \sys_get_temp_dir() . '/' . \str_replace('.', '', \uniqid('partitioned_', true));

        (new Flow())
            ->process(
                new Rows(
                    Row::create(int_entry('id', 1), int_entry('group', 1)),
                    Row::create(int_entry('id', 2), int_entry('group', 1)),
                    Row::create(int_entry('id', 3), int_entry('group', 2)),
                    Row::create(int_entry('id', 4), int_entry('group', 2)),
                )
            )
            ->partitionBy('group')
            ->load(to_csv($path))
            ->run();

        $partitions = \array_values(\array_diff(\scandir($path), ['..', '.']));

        $this->assertSame(
            [
                'group=1',
                'group=2',
            ],
            $partitions
        );

        $group1 = \array_values(\array_diff(\scandir($path . DIRECTORY_SEPARATOR . 'group=1'), ['..', '.']))[0];
        $group2 = \array_values(\array_diff(\scandir($path . DIRECTORY_SEPARATOR . 'group=2'), ['..', '.']))[0];

        $this->assertStringContainsString(
            <<<'CSV'
id,group
1,1
2,1
CSV,
            \file_get_contents($path . DIRECTORY_SEPARATOR . 'group=1' . DIRECTORY_SEPARATOR . $group1)
        );

        $this->assertStringContainsString(
            <<<'CSV'
id,group
3,2
4,2
CSV,
            \file_get_contents($path . DIRECTORY_SEPARATOR . 'group=2' . DIRECTORY_SEPARATOR . $group2)
        );

        $this->cleanDirectory($path);
    }

    public function test_loading_overwrite_csv() : void
    {
        $path = \sys_get_temp_dir() . '/' . \uniqid('flow_php_etl_csv_loader', true) . '.csv';

        (new Flow())
            ->process(
                new Rows(
                    Row::create(int_entry('id', 1), str_entry('name', 'Tomek')),
                    Row::create(int_entry('id', 2), str_entry('name', 'Dawid')),
                    Row::create(int_entry('id', 3), str_entry('name', 'Norbert')),
                )
            )
            ->load(to_csv($path))
            ->run();

        (new Flow())
            ->process(
                new Rows(
                    Row::create(int_entry('id', 1), str_entry('name', 'Norbert')),
                    Row::create(int_entry('id', 2), str_entry('name', 'Tomek')),
                    Row::create(int_entry('id', 3), str_entry('name', 'Dawid')),
                )
            )
            ->saveMode(overwrite())
            ->load(to_csv($path))
            ->run();

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

    public function test_using_pattern_path() : void
    {
        $this->expectExceptionMessage("CSVLoader path can't be pattern, given: /path/*/pattern.csv");

        to_csv(new Path('/path/*/pattern.csv'));
    }

    /**
     * @param string $path
     */
    private function cleanDirectory(string $path) : void
    {
        if (\file_exists($path) && \is_dir($path)) {
            $files = \array_values(\array_diff(\scandir($path), ['..', '.']));

            foreach ($files as $file) {
                if (\is_file($path . DIRECTORY_SEPARATOR . $file)) {
                    $this->removeFile($path . DIRECTORY_SEPARATOR . $file);
                } else {
                    $this->cleanDirectory($path . DIRECTORY_SEPARATOR . $file);
                }
            }

            \rmdir($path);
        }
    }

    /**
     * @param string $path
     */
    private function removeFile(string $path) : void
    {
        if (\file_exists($path)) {
            \unlink($path);
        }
    }
}
