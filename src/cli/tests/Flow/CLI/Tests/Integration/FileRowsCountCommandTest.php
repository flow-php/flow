<?php

declare(strict_types=1);

namespace Flow\CLI\Tests\Integration;

use Flow\CLI\Command\FileRowsCountCommand;
use Flow\CLI\Command\FileSchemaCommand;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use Symfony\Component\Console\Application;
use Symfony\Component\Console\Input\ArrayInput;
use Symfony\Component\Console\Output\BufferedOutput;
use Symfony\Component\Console\Tester\CommandTester;

final class FileRowsCountCommandTest extends TestCase
{
    public function test_a_failing_run_clears_the_status_line(): void
    {
        $tester = new CommandTester(new FileRowsCountCommand('count'));

        try {
            $tester->execute([
                'input-file' => __DIR__ . '/Fixtures/orders.csv',
                '--config' => __DIR__ . '/Fixtures/failing-heartbeat-config.php',
            ], ['capture_stderr_separately' => true, 'decorated' => true]);
            static::fail('The run must fail.');
        } catch (RuntimeException $e) {
            static::assertSame('lease lost', $e->getMessage());
        }

        static::assertStringEndsWith("\x1B[1G\x1B[2K", $tester->getErrorOutput());
    }

    public function test_a_plain_output_gets_only_the_count(): void
    {
        $output = new BufferedOutput();

        (new FileRowsCountCommand('count'))->run(new ArrayInput([
            'input-file' => __DIR__ . '/Fixtures/orders.csv',
            '--config' => __DIR__ . '/Fixtures/stepping-clock-config.php',
        ]), $output);

        static::assertSame('43', $output->fetch());
    }

    public function test_progress_goes_to_stderr(): void
    {
        $tester = new CommandTester(new FileRowsCountCommand('count'));

        $tester->execute([
            'input-file' => __DIR__ . '/Fixtures/orders.csv',
            '--config' => __DIR__ . '/Fixtures/stepping-clock-config.php',
        ], ['capture_stderr_separately' => true]);

        static::assertSame('43', $tester->getDisplay());
        static::assertMatchesRegularExpression('/^ read: 43 rows · 43 read · /m', $tester->getErrorOutput());
    }

    public function test_count_rows_csv(): void
    {
        $tester = new CommandTester(new FileRowsCountCommand('count'));

        $tester->execute(['input-file' => __DIR__ . '/Fixtures/orders.csv'], ['capture_stderr_separately' => true]);

        $tester->assertCommandIsSuccessful();

        static::assertSame('43', $tester->getDisplay());
    }

    public function test_count_rows_excel(): void
    {
        $tester = new CommandTester(new FileRowsCountCommand('count'));

        $tester->execute(['input-file' => __DIR__ . '/Fixtures/orders.xlsx'], ['capture_stderr_separately' => true]);

        $tester->assertCommandIsSuccessful();

        static::assertSame('43', $tester->getDisplay());
    }

    public function test_count_rows_json(): void
    {
        $tester = new CommandTester(new FileRowsCountCommand('count'));

        $tester->execute(['input-file' => __DIR__ . '/Fixtures/orders.json'], ['capture_stderr_separately' => true]);

        $tester->assertCommandIsSuccessful();

        static::assertSame('10000', $tester->getDisplay());
    }

    public function test_count_rows_parquet(): void
    {
        $tester = new CommandTester(new FileRowsCountCommand('count'));

        $tester->execute(['input-file' => __DIR__ . '/Fixtures/orders.parquet'], ['capture_stderr_separately' => true]);

        $tester->assertCommandIsSuccessful();

        static::assertSame('1000', $tester->getDisplay());
    }

    public function test_count_rows_text(): void
    {
        $tester = new CommandTester(new FileRowsCountCommand('count'));

        $tester->execute(['input-file' => __DIR__ . '/Fixtures/orders.txt'], ['capture_stderr_separately' => true]);

        $tester->assertCommandIsSuccessful();

        static::assertSame('44', $tester->getDisplay());
    }

    public function test_count_rows_with_large_offset(): void
    {
        $tester = new CommandTester(new FileRowsCountCommand('count'));

        $tester->execute([
            'input-file' => __DIR__ . '/Fixtures/orders.csv',
            '--input-file-offset' => 1000,
        ], ['capture_stderr_separately' => true]);

        $tester->assertCommandIsSuccessful();

        // Offset larger than file should result in 0 rows
        static::assertSame('0', $tester->getDisplay());
    }

    public function test_count_rows_with_offset_and_limit(): void
    {
        $tester = new CommandTester(new FileRowsCountCommand('count'));

        $tester->execute([
            'input-file' => __DIR__ . '/Fixtures/orders.csv',
            '--input-file-offset' => 2,
            '--input-file-limit' => 5,
        ], ['capture_stderr_separately' => true]);

        $tester->assertCommandIsSuccessful();

        // CSV has 43 total rows, with offset 2 and limit 5 should count 3 rows (limit applies first, then offset)
        static::assertSame('3', $tester->getDisplay());
    }

    public function test_count_rows_with_offset_csv(): void
    {
        $tester = new CommandTester(new FileRowsCountCommand('count'));

        $tester->execute([
            'input-file' => __DIR__ . '/Fixtures/orders.csv',
            '--input-file-offset' => 5,
        ], ['capture_stderr_separately' => true]);

        $tester->assertCommandIsSuccessful();

        // CSV has 43 total rows, with offset 5 should count 38 rows
        static::assertSame('38', $tester->getDisplay());
    }

    public function test_count_rows_with_zero_offset(): void
    {
        $tester = new CommandTester(new FileRowsCountCommand('count'));

        $tester->execute([
            'input-file' => __DIR__ . '/Fixtures/orders.csv',
            '--input-file-offset' => 0,
        ], ['capture_stderr_separately' => true]);

        $tester->assertCommandIsSuccessful();

        // Zero offset should behave same as no offset
        static::assertSame('43', $tester->getDisplay());
    }

    public function test_count_rows_xml(): void
    {
        $tester = new CommandTester(new FileRowsCountCommand('count'));

        $tester->execute(['input-file' => __DIR__ . '/Fixtures/orders.xml', '--input-xml-node-path' => 'root/row'], [
            'capture_stderr_separately' => true,
        ]);

        $tester->assertCommandIsSuccessful();

        static::assertSame('10000', $tester->getDisplay());
    }

    public function test_file_rows_count_command_registers_under_its_own_name_beside_file_schema_command(): void
    {
        $application = new Application();
        $application->addCommands([new FileSchemaCommand(), new FileRowsCountCommand()]);

        static::assertInstanceOf(FileSchemaCommand::class, $application->find('file:schema'));
        static::assertInstanceOf(FileRowsCountCommand::class, $application->find('file:rows:count'));
        static::assertInstanceOf(FileRowsCountCommand::class, $application->find('count'));
    }
}
