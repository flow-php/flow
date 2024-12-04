<?php

declare(strict_types=1);

namespace Flow\CLI\Tests\Integration;

use Flow\CLI\Command\FileRowsCountCommand;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Console\Tester\CommandTester;

final class FileRowsCountCommandTest extends TestCase
{
    public function test_count_rows_csv() : void
    {
        $tester = new CommandTester(new FileRowsCountCommand('count'));

        $tester->execute(['input-file' => __DIR__ . '/Fixtures/orders.csv']);

        $tester->assertCommandIsSuccessful();

        self::assertSame('43', $tester->getDisplay());
    }

    public function test_count_rows_json() : void
    {
        $tester = new CommandTester(new FileRowsCountCommand('count'));

        $tester->execute(['input-file' => __DIR__ . '/Fixtures/orders.json']);

        $tester->assertCommandIsSuccessful();

        self::assertSame('10000', $tester->getDisplay());
    }

    public function test_count_rows_parquet() : void
    {
        $tester = new CommandTester(new FileRowsCountCommand('count'));

        $tester->execute(['input-file' => __DIR__ . '/Fixtures/orders.parquet']);

        $tester->assertCommandIsSuccessful();

        self::assertSame('1000', $tester->getDisplay());
    }

    public function test_count_rows_text() : void
    {
        $tester = new CommandTester(new FileRowsCountCommand('count'));

        $tester->execute(['input-file' => __DIR__ . '/Fixtures/orders.txt']);

        $tester->assertCommandIsSuccessful();

        self::assertSame('44', $tester->getDisplay());
    }

    public function test_count_rows_xml() : void
    {
        $tester = new CommandTester(new FileRowsCountCommand('count'));

        $tester->execute(['input-file' => __DIR__ . '/Fixtures/orders.xml', '--input-xml-node-path' => 'root/row']);

        $tester->assertCommandIsSuccessful();

        self::assertSame('10000', $tester->getDisplay());
    }
}
