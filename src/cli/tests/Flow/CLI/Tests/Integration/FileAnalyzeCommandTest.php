<?php

declare(strict_types=1);

namespace Flow\CLI\Tests\Integration;

use Flow\CLI\Command\{FileAnalyzeCommand};
use Flow\ETL\Tests\FlowTestCase;
use Symfony\Component\Console\Application;
use Symfony\Component\Console\Tester\CommandTester;

final class FileAnalyzeCommandTest extends FlowTestCase
{
    public function test_read_rows_csv() : void
    {
        $application = new Application();
        $application->add(new FileAnalyzeCommand());
        $tester = new CommandTester($application->get('file:analyze'));

        $tester->execute(['input-file' => __DIR__ . '/Fixtures/orders.csv', '--input-file-limit' => 5, '--stats-schema' => true, '--stats-columns' => true, '--schema-auto-cast' => true]);

        $tester->assertCommandIsSuccessful();

        self::assertStringContainsString(
            <<<'OUTPUT'
Analyzing File
==============

 [INFO] File path: orders.csv
OUTPUT,
            $tester->getDisplay()
        );

        self::assertStringContainsString(
            <<<'OUTPUT'
┌────────────┬───────────────────────────────────────────────────────────────┬──────────┬──────────┐
│ Name       │ Type                                                          │ Nullable │ Metadata │
├────────────┼───────────────────────────────────────────────────────────────┼──────────┼──────────┤
│ order_id   │ uuid                                                          │ false    │ {}       │
│ created_at │ datetime                                                      │ false    │ {}       │
│ updated_at │ datetime                                                      │ false    │ {}       │
│ discount   │ float                                                         │ true     │ {}       │
│ address    │ map<string, string>                                           │ false    │ {}       │
│ notes      │ list<string>                                                  │ false    │ {}       │
│ items      │ list<structure{sku: string, quantity: integer, price: float}> │ false    │ {}       │
└────────────┴───────────────────────────────────────────────────────────────┴──────────┴──────────┘

Columns
-------

┌────────────┬───────────────────────────────────────────────────────────────┬───────┬─────────────────┬───────────────────────────┬───────────────────────────┬────────────┬────────────┬────────────────────┬────────────────────┐
│ Name       │ Type                                                          │ Nulls │ Distinct Values │ Min                       │ Max                       │ Min Length │ Max Length │ Min Elements Count │ Max Elements Count │
├────────────┼───────────────────────────────────────────────────────────────┼───────┼─────────────────┼───────────────────────────┼───────────────────────────┼────────────┼────────────┼────────────────────┼────────────────────┤
│ order_id   │ uuid                                                          │ 0     │ 5               │ -                         │ -                         │ -          │ -          │ -                  │ -                  │
│ created_at │ datetime                                                      │ 0     │ 5               │ 2024-02-23T19:18:53+00:00 │ 2024-06-17T19:24:49+00:00 │ -          │ -          │ -                  │ -                  │
│ updated_at │ datetime                                                      │ 0     │ 5               │ 2024-02-23T19:18:53+00:00 │ 2024-06-17T19:24:49+00:00 │ -          │ -          │ -                  │ -                  │
│ discount   │ float                                                         │ 2     │ 3               │ 12.45                     │ 47.10                     │ -          │ -          │ -                  │ -                  │
│ address    │ map<string, string>                                           │ 0     │ 5               │ -                         │ -                         │ -          │ -          │ 4                  │ 4                  │
│ notes      │ list<string>                                                  │ 0     │ 5               │ -                         │ -                         │ -          │ -          │ 1                  │ 5                  │
│ items      │ list<structure{sku: string, quantity: integer, price: float}> │ 0     │ 5               │ -                         │ -                         │ -          │ -          │ 2                  │ 4                  │
└────────────┴───────────────────────────────────────────────────────────────┴───────┴─────────────────┴───────────────────────────┴───────────────────────────┴────────────┴────────────┴────────────────────┴────────────────────┘
OUTPUT,
            $tester->getDisplay()
        );

        self::assertStringContainsString('Analyzed Rows', $tester->getDisplay());

        self::assertStringContainsString('Execution Time', $tester->getDisplay());
    }

    public function test_read_rows_csv_without_schema() : void
    {
        $application = new Application();
        $application->add(new FileAnalyzeCommand());
        $tester = new CommandTester($application->get('file:analyze'));

        $tester->execute(['input-file' => __DIR__ . '/Fixtures/orders.csv', '--input-file-limit' => 5, '--stats-columns' => true, '--schema-auto-cast' => true]);

        $tester->assertCommandIsSuccessful();

        self::assertStringContainsString(
            <<<'OUTPUT'
Analyzing File
==============

 [INFO] File path: orders.csv
OUTPUT,
            $tester->getDisplay()
        );

        self::assertStringContainsString(
            <<<'OUTPUT'
Columns
-------

┌────────────┬───────────────────────────────────────────────────────────────┬───────┬─────────────────┬───────────────────────────┬───────────────────────────┬────────────┬────────────┬────────────────────┬────────────────────┐
│ Name       │ Type                                                          │ Nulls │ Distinct Values │ Min                       │ Max                       │ Min Length │ Max Length │ Min Elements Count │ Max Elements Count │
├────────────┼───────────────────────────────────────────────────────────────┼───────┼─────────────────┼───────────────────────────┼───────────────────────────┼────────────┼────────────┼────────────────────┼────────────────────┤
│ order_id   │ uuid                                                          │ 0     │ 5               │ -                         │ -                         │ -          │ -          │ -                  │ -                  │
│ created_at │ datetime                                                      │ 0     │ 5               │ 2024-02-23T19:18:53+00:00 │ 2024-06-17T19:24:49+00:00 │ -          │ -          │ -                  │ -                  │
│ updated_at │ datetime                                                      │ 0     │ 5               │ 2024-02-23T19:18:53+00:00 │ 2024-06-17T19:24:49+00:00 │ -          │ -          │ -                  │ -                  │
│ discount   │ float                                                         │ 2     │ 3               │ 12.45                     │ 47.10                     │ -          │ -          │ -                  │ -                  │
│ address    │ map<string, string>                                           │ 0     │ 5               │ -                         │ -                         │ -          │ -          │ 4                  │ 4                  │
│ notes      │ list<string>                                                  │ 0     │ 5               │ -                         │ -                         │ -          │ -          │ 1                  │ 5                  │
│ items      │ list<structure{sku: string, quantity: integer, price: float}> │ 0     │ 5               │ -                         │ -                         │ -          │ -          │ 2                  │ 4                  │
└────────────┴───────────────────────────────────────────────────────────────┴───────┴─────────────────┴───────────────────────────┴───────────────────────────┴────────────┴────────────┴────────────────────┴────────────────────┘
OUTPUT,
            $tester->getDisplay()
        );

        self::assertStringContainsString('Analyzed Rows', $tester->getDisplay());

        self::assertStringContainsString('Execution Time', $tester->getDisplay());
    }
}
