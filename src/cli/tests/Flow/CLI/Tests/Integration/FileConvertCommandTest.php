<?php

declare(strict_types=1);

namespace Flow\CLI\Tests\Integration;

use Flow\CLI\Command\FileConvertCommand;
use Flow\CLI\Command\FileReadCommand;
use InvalidArgumentException;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use Symfony\Component\Console\Application;
use Symfony\Component\Console\Input\ArrayInput;
use Symfony\Component\Console\Output\BufferedOutput;
use Symfony\Component\Console\Tester\CommandTester;

use function bin2hex;
use function file_exists;
use function Flow\ETL\Adapter\CSV\from_csv;
use function Flow\ETL\DSL\data_frame;
use function random_bytes;
use function unlink;

final class FileConvertCommandTest extends TestCase
{
    public function test_a_failing_run_clears_the_status_line(): void
    {
        $output = __DIR__ . '/var/' . bin2hex(random_bytes(16)) . '.csv';
        $tester = new CommandTester(new FileConvertCommand('convert'));

        try {
            $tester->execute([
                'input-file' => __DIR__ . '/Fixtures/orders.csv',
                'output-file' => $output,
                '--config' => __DIR__ . '/Fixtures/failing-heartbeat-config.php',
            ], ['capture_stderr_separately' => true, 'decorated' => true]);
            static::fail('The run must fail.');
        } catch (RuntimeException $e) {
            static::assertSame('lease lost', $e->getMessage());
        }

        static::assertStringEndsWith("\x1B[1G\x1B[2K", $tester->getErrorOutput());

        if (file_exists($output)) {
            unlink($output);
        }
    }

    public function test_a_plain_output_gets_no_progress(): void
    {
        $output = __DIR__ . '/var/' . bin2hex(random_bytes(16)) . '.csv';
        $buffered = new BufferedOutput();

        (new FileConvertCommand('convert'))->run(new ArrayInput([
            'input-file' => __DIR__ . '/Fixtures/orders.csv',
            'output-file' => $output,
            '--config' => __DIR__ . '/Fixtures/stepping-clock-config.php',
        ]), $buffered);

        static::assertStringNotContainsString(' read: ', $buffered->fetch());
        unlink($output);
    }

    /**
     * @param array<string> $options
     */
    #[TestWith(['csv', 'parquet'])]
    #[TestWith(['csv', 'json'])]
    #[TestWith(['csv', 'xml'])]
    #[TestWith(['xlsx', 'json'])]
    #[TestWith(['xlsx', 'xml'])]
    #[TestWith(['xlsx', 'csv'])]
    #[TestWith(['json', 'parquet'])]
    #[TestWith(['json', 'xml'])]
    #[TestWith(['json', 'csv'])]
    #[TestWith(['parquet', 'json'])]
    #[TestWith(['parquet', 'xml'])]
    #[TestWith(['parquet', 'csv'])]
    #[TestWith(['xml', 'parquet', ['--input-xml-node-path' => 'root/row']])]
    #[TestWith(['xml', 'json', ['--input-xml-node-path' => 'root/row']])]
    #[TestWith(['xml', 'csv', ['--input-xml-node-path' => 'root/row']])]
    /**
     * @param string $inputFormat
     * @param string $outputFormat
     * @param array<string, mixed> $options
     */
    public function test_convert(string $inputFormat, string $outputFormat, array $options = []): void
    {
        /** @var array<string, mixed> $options */
        $output = __DIR__ . '/var/' . bin2hex(random_bytes(16)) . '.' . $outputFormat;

        if (file_exists($output)) {
            unlink($output);
        }

        $tester = new CommandTester(new FileConvertCommand('convert'));

        $tester->execute(
            array_merge([
                'input-file' => __DIR__ . '/Fixtures/orders.' . $inputFormat,
                'output-file' => $output,
                '--input-file-limit' => 5,
            ], $options),
            ['capture_stderr_separately' => true],
        );

        $tester->assertCommandIsSuccessful();

        static::assertFileExists($output);
        unlink($output);
    }

    public function test_convert_with_sample_size_writes_the_truncated_value(): void
    {
        $output = __DIR__ . '/var/' . bin2hex(random_bytes(16)) . '.csv';

        $tester = new CommandTester(new FileConvertCommand('convert'));

        $tester->execute([
            'input-file' => __DIR__ . '/Fixtures/inference/widening.csv',
            'output-file' => $output,
            '--schema-sample-size' => 1,
        ], ['capture_stderr_separately' => true]);

        $tester->assertCommandIsSuccessful();

        // The frozen ?integer schema truncates 1.5 - integer::cast(1.5) is 1
        static::assertSame([['a' => 1], ['a' => 1]], data_frame()->read(from_csv($output))->fetch()->toArray());

        unlink($output);
    }

    public function test_cli_rejects_a_remote_input_path(): void
    {
        $tester = new CommandTester(new FileConvertCommand('convert'));

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('The Flow CLI reads and writes "file://" paths only');

        $tester->execute([
            'input-file' => 'aws-s3://bucket/orders.csv',
            'output-file' => __DIR__ . '/var/' . bin2hex(random_bytes(16)) . '.json',
        ], ['capture_stderr_separately' => true]);
    }

    public function test_cli_rejects_a_remote_output_path(): void
    {
        $tester = new CommandTester(new FileConvertCommand('convert'));

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('The Flow CLI reads and writes "file://" paths only');

        $tester->execute([
            'input-file' => __DIR__ . '/Fixtures/orders.csv',
            'output-file' => 'aws-s3://bucket/orders.parquet',
        ], ['capture_stderr_separately' => true]);
    }

    public function test_convert_with_offset(): void
    {
        $output = __DIR__ . '/var/' . bin2hex(random_bytes(16)) . '.json';

        if (file_exists($output)) {
            unlink($output);
        }

        $tester = new CommandTester(new FileConvertCommand('convert'));

        $tester->execute([
            'input-file' => __DIR__ . '/Fixtures/orders.csv',
            'output-file' => $output,
            '--input-file-limit' => 3,
            '--input-file-offset' => 2,
            '--output-overwrite' => true,
        ], ['capture_stderr_separately' => true]);

        $tester->assertCommandIsSuccessful();

        static::assertFileExists($output);

        // Read the converted file to verify offset was applied
        $content = file_get_contents($output);
        static::assertNotFalse($content);

        // Should contain the third row (after offset of 2) but not the first two rows
        static::assertStringContainsString('6315f9e2-86bf-3321-a', $content); // Third row
        static::assertStringNotContainsString('e13d7098-5a78-3389-9', $content); // First row should not be there
        static::assertStringNotContainsString('947df050-3abb-3f5a-9', $content); // Second row should not be there

        unlink($output);
    }

    public function test_convert_with_offset_and_limit(): void
    {
        $output = __DIR__ . '/var/' . bin2hex(random_bytes(16)) . '.json';

        if (file_exists($output)) {
            unlink($output);
        }

        $tester = new CommandTester(new FileConvertCommand('convert'));

        $tester->execute([
            'input-file' => __DIR__ . '/Fixtures/orders.csv',
            'output-file' => $output,
            '--input-file-limit' => 3,
            '--input-file-offset' => 1,
            '--output-overwrite' => true,
        ], ['capture_stderr_separately' => true]);

        $tester->assertCommandIsSuccessful();

        static::assertFileExists($output);

        // Read the converted file to verify offset + limit was applied
        $content = file_get_contents($output);
        static::assertNotFalse($content);

        // Should contain second and third rows (offset 1, limit 3 gives us 2 rows after offset)
        static::assertStringContainsString('947df050-3abb-3f5a-9', $content); // Second row
        static::assertStringContainsString('6315f9e2-86bf-3321-a', $content); // Third row
        static::assertStringNotContainsString('e13d7098-5a78-3389-9', $content); // First row should not be there

        unlink($output);
    }

    public function test_file_convert_command_registers_under_its_own_name_beside_file_read_command(): void
    {
        $application = new Application();
        $application->addCommands([new FileReadCommand(), new FileConvertCommand()]);

        static::assertInstanceOf(FileReadCommand::class, $application->find('file:read'));
        static::assertInstanceOf(FileConvertCommand::class, $application->find('file:convert'));
        static::assertInstanceOf(FileConvertCommand::class, $application->find('convert'));
    }
}
