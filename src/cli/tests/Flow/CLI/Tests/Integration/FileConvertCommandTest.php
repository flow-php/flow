<?php

declare(strict_types=1);

namespace Flow\CLI\Tests\Integration;

use Flow\CLI\Command\FileConvertCommand;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Console\Tester\CommandTester;

final class FileConvertCommandTest extends TestCase
{
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
    public function test_convert(string $inputFormat, string $outputFormat, array $options = []) : void
    {
        /** @var array<string, mixed> $options */
        $output = __DIR__ . '/var/' . bin2hex(random_bytes(16)) . '.' . $outputFormat;

        if (\file_exists($output)) {
            \unlink($output);
        }

        $tester = new CommandTester(new FileConvertCommand('convert'));

        $tester->execute(
            array_merge(
                [
                    'input-file' => __DIR__ . '/Fixtures/orders.' . $inputFormat,
                    'output-file' => $output,
                    '--input-file-limit' => 5,
                    '--schema-auto-cast' => true,
                ],
                $options
            )
        );

        $tester->assertCommandIsSuccessful();

        self::assertFileExists($output);
        unlink($output);
    }

    public function test_convert_with_offset() : void
    {
        $output = __DIR__ . '/var/' . bin2hex(random_bytes(16)) . '.json';

        if (\file_exists($output)) {
            \unlink($output);
        }

        $tester = new CommandTester(new FileConvertCommand('convert'));

        $tester->execute([
            'input-file' => __DIR__ . '/Fixtures/orders.csv',
            'output-file' => $output,
            '--input-file-limit' => 3,
            '--input-file-offset' => 2,
            '--output-overwrite' => true,
        ]);

        $tester->assertCommandIsSuccessful();

        self::assertFileExists($output);

        // Read the converted file to verify offset was applied
        $content = file_get_contents($output);
        self::assertNotFalse($content);

        // Should contain the third row (after offset of 2) but not the first two rows
        self::assertStringContainsString('6315f9e2-86bf-3321-a', $content); // Third row
        self::assertStringNotContainsString('e13d7098-5a78-3389-9', $content); // First row should not be there
        self::assertStringNotContainsString('947df050-3abb-3f5a-9', $content); // Second row should not be there

        unlink($output);
    }

    public function test_convert_with_offset_and_limit() : void
    {
        $output = __DIR__ . '/var/' . bin2hex(random_bytes(16)) . '.json';

        if (\file_exists($output)) {
            \unlink($output);
        }

        $tester = new CommandTester(new FileConvertCommand('convert'));

        $tester->execute([
            'input-file' => __DIR__ . '/Fixtures/orders.csv',
            'output-file' => $output,
            '--input-file-limit' => 3,
            '--input-file-offset' => 1,
            '--output-overwrite' => true,
        ]);

        $tester->assertCommandIsSuccessful();

        self::assertFileExists($output);

        // Read the converted file to verify offset + limit was applied
        $content = file_get_contents($output);
        self::assertNotFalse($content);

        // Should contain second and third rows (offset 1, limit 3 gives us 2 rows after offset)
        self::assertStringContainsString('947df050-3abb-3f5a-9', $content); // Second row
        self::assertStringContainsString('6315f9e2-86bf-3321-a', $content); // Third row
        self::assertStringNotContainsString('e13d7098-5a78-3389-9', $content); // First row should not be there

        unlink($output);
    }
}
