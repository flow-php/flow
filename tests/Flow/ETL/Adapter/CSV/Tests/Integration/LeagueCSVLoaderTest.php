<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV\Tests\Integration;

use Flow\ETL\Adapter\CSV\LeagueCSVLoader;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use League\Csv\Writer;
use PHPUnit\Framework\TestCase;

final class LeagueCSVLoaderTest extends TestCase
{
    public function test_loading_csv_files() : void
    {
        $path = \sys_get_temp_dir() . '/' . \uniqid('flow_php_etl_csv_loader', true) . '.csv';
        $writer = Writer::createFromPath($path, 'w+');

        $loader = new LeagueCSVLoader($writer);

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
            unset($path);
        }
    }
}
