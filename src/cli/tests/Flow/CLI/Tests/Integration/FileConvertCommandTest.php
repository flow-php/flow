<?php

declare(strict_types=1);

namespace Flow\CLI\Tests\Integration;

use Flow\CLI\Command\FileConvertCommand;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Console\Tester\CommandTester;

final class FileConvertCommandTest extends TestCase
{
    #[TestWith(['csv', 'parquet'])]
    #[TestWith(['csv', 'json'])]
    #[TestWith(['csv', 'xml'])]
    #[TestWith(['json', 'parquet'])]
    #[TestWith(['json', 'xml'])]
    #[TestWith(['json', 'csv'])]
    #[TestWith(['parquet', 'json'])]
    #[TestWith(['parquet', 'xml'])]
    #[TestWith(['parquet', 'csv'])]
    #[TestWith(['xml', 'parquet', ['--input-xml-node-path' => 'root/row']])]
    #[TestWith(['xml', 'json', ['--input-xml-node-path' => 'root/row']])]
    #[TestWith(['xml', 'csv', ['--input-xml-node-path' => 'root/row']])]
    public function test_convert(string $inputFormat, string $outputFormat, array $options = []) : void
    {
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
}
