<?php

declare(strict_types=1);

namespace Flow\CLI\Tests\Integration;

use Flow\CLI\Command\FileAnalyzeCommand;
use Flow\ETL\Tests\CommandOutputNormalizer;
use Flow\ETL\Tests\FlowTestCase;
use Symfony\Component\Console\Tester\CommandTester;

final class FileAnalyzeCommandTest extends FlowTestCase
{
    use CommandOutputNormalizer;

    public function test_read_rows_csv(): void
    {
        $tester = new CommandTester(new FileAnalyzeCommand('file:analyze'));

        $tester->execute([
            'input-file' => __DIR__ . '/Fixtures/orders.csv',
            '--input-file-limit' => 5,
            '--stats-schema' => true,
            '--stats-columns' => true,
            '--schema-auto-cast' => true,
        ]);

        $tester->assertCommandIsSuccessful();

        self::assertCommandOutputContains(<<<'OUTPUT'
            Analyzing File
            ==============

             [INFO] File path: orders.csv
            OUTPUT, $tester->getDisplay());

        self::assertCommandOutputContains(<<<'OUTPUT'
            ┌────────────┬──────────┬──────────┬──────────┐
            │ Name       │ Type     │ Nullable │ Metadata │
            ├────────────┼──────────┼──────────┼──────────┤
            │ order_id   │ uuid     │ false    │ {}       │
            │ created_at │ datetime │ false    │ {}       │
            │ updated_at │ datetime │ false    │ {}       │
            │ discount   │ float    │ true     │ {}       │
            │ address    │ json     │ false    │ {}       │
            │ notes      │ json     │ false    │ {}       │
            │ items      │ json     │ false    │ {}       │
            └────────────┴──────────┴──────────┴──────────┘

            Columns
            -------

            ┌────────────┬──────────┬───────┬─────────────────┬───────────────────────────┬───────────────────────────┬────────────┬────────────┬────────────────────┬────────────────────┐
            │ Name       │ Type     │ Nulls │ Distinct Values │ Min                       │ Max                       │ Min Length │ Max Length │ Min Elements Count │ Max Elements Count │
            ├────────────┼──────────┼───────┼─────────────────┼───────────────────────────┼───────────────────────────┼────────────┼────────────┼────────────────────┼────────────────────┤
            │ order_id   │ uuid     │ 0     │ 5               │ -                         │ -                         │ -          │ -          │ -                  │ -                  │
            │ created_at │ datetime │ 0     │ 5               │ 2024-02-23T19:18:53+00:00 │ 2024-06-17T19:24:49+00:00 │ -          │ -          │ -                  │ -                  │
            │ updated_at │ datetime │ 0     │ 5               │ 2024-02-23T19:18:53+00:00 │ 2024-06-17T19:24:49+00:00 │ -          │ -          │ -                  │ -                  │
            │ discount   │ float    │ 2     │ 3               │ 12.45                     │ 47.10                     │ -          │ -          │ -                  │ -                  │
            │ address    │ json     │ 0     │ 0               │ -                         │ -                         │ -          │ -          │ -                  │ -                  │
            │ notes      │ json     │ 0     │ 0               │ -                         │ -                         │ -          │ -          │ -                  │ -                  │
            │ items      │ json     │ 0     │ 0               │ -                         │ -                         │ -          │ -          │ -                  │ -                  │
            └────────────┴──────────┴───────┴─────────────────┴───────────────────────────┴───────────────────────────┴────────────┴────────────┴────────────────────┴────────────────────┘
            OUTPUT, $tester->getDisplay());

        self::assertCommandOutputContains('Analyzed Rows', $tester->getDisplay());

        self::assertCommandOutputContains('Execution Time', $tester->getDisplay());
    }

    public function test_read_rows_csv_without_schema(): void
    {
        $tester = new CommandTester(new FileAnalyzeCommand('file:analyze'));

        $tester->execute([
            'input-file' => __DIR__ . '/Fixtures/orders.csv',
            '--input-file-limit' => 5,
            '--stats-columns' => true,
            '--schema-auto-cast' => true,
        ]);

        $tester->assertCommandIsSuccessful();

        self::assertCommandOutputContains(<<<'OUTPUT'
            Analyzing File
            ==============

             [INFO] File path: orders.csv
            OUTPUT, $tester->getDisplay());

        self::assertCommandOutputContains(<<<'OUTPUT'
            Columns
            -------

            ┌────────────┬──────────┬───────┬─────────────────┬───────────────────────────┬───────────────────────────┬────────────┬────────────┬────────────────────┬────────────────────┐
            │ Name       │ Type     │ Nulls │ Distinct Values │ Min                       │ Max                       │ Min Length │ Max Length │ Min Elements Count │ Max Elements Count │
            ├────────────┼──────────┼───────┼─────────────────┼───────────────────────────┼───────────────────────────┼────────────┼────────────┼────────────────────┼────────────────────┤
            │ order_id   │ uuid     │ 0     │ 5               │ -                         │ -                         │ -          │ -          │ -                  │ -                  │
            │ created_at │ datetime │ 0     │ 5               │ 2024-02-23T19:18:53+00:00 │ 2024-06-17T19:24:49+00:00 │ -          │ -          │ -                  │ -                  │
            │ updated_at │ datetime │ 0     │ 5               │ 2024-02-23T19:18:53+00:00 │ 2024-06-17T19:24:49+00:00 │ -          │ -          │ -                  │ -                  │
            │ discount   │ float    │ 2     │ 3               │ 12.45                     │ 47.10                     │ -          │ -          │ -                  │ -                  │
            │ address    │ json     │ 0     │ 0               │ -                         │ -                         │ -          │ -          │ -                  │ -                  │
            │ notes      │ json     │ 0     │ 0               │ -                         │ -                         │ -          │ -          │ -                  │ -                  │
            │ items      │ json     │ 0     │ 0               │ -                         │ -                         │ -          │ -          │ -                  │ -                  │
            └────────────┴──────────┴───────┴─────────────────┴───────────────────────────┴───────────────────────────┴────────────┴────────────┴────────────────────┴────────────────────┘
            OUTPUT, $tester->getDisplay());

        self::assertCommandOutputContains('Analyzed Rows', $tester->getDisplay());

        self::assertCommandOutputContains('Execution Time', $tester->getDisplay());
    }
}
