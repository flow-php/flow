<?php

declare(strict_types=1);

namespace Flow\ParquetViewer\Tests\Integration;

use Flow\ETL\Tests\CommandOutputNormalizer;
use Flow\ParquetViewer\Parquet;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Console\Input\ArrayInput;
use Symfony\Component\Console\Output\BufferedOutput;
use Symfony\Component\Console\Style\{OutputStyle, SymfonyStyle};
use Symfony\Component\Console\Tester\ApplicationTester;

final class ReadMetadataTest extends TestCase
{
    use CommandOutputNormalizer;

    public function test_reading_metadata_from_non_json_file() : void
    {
        $application = new Parquet();
        $application->setAutoExit(false);
        $application->setCatchExceptions(false);

        $path = \realpath(__DIR__ . '/../../Fixtures/flow.json');

        $tester = new ApplicationTester($application);
        $tester->run([
            'command' => 'read:metadata',
            'file' => $path,
        ]);

        $expected = $this->captureConsoleOutput(
            fn (OutputStyle $io) => $io->error("File \"{$path}\" is not a valid parquet file")
        );

        self::assertCommandOutputContains(self::normalizeCommandOutput($expected), $tester->getDisplay());
        self::assertSame(1, $tester->getStatusCode());
    }

    public function test_reading_metadata_from_parquet_file() : void
    {
        $application = new Parquet();
        $application->setAutoExit(false);
        $application->setCatchExceptions(false);

        $path = \realpath(__DIR__ . '/../../Fixtures/flow.parquet');

        $tester = new ApplicationTester($application);
        $tester->run([
            'command' => 'read:metadata',
            'file' => $path,
            '--row-groups' => 1,
            '--page-headers' => 1,
            '--column-chunks' => 1,
            '--statistics' => 1,
        ]);

        self::assertCommandOutputContains('Metadata', $tester->getDisplay());
        self::assertCommandOutputContains('Row Groups', $tester->getDisplay());
        self::assertCommandOutputContains('Column Chunks', $tester->getDisplay());
        self::assertCommandOutputContains('Column Chunks Statistics', $tester->getDisplay());
        self::assertCommandOutputContains('Page Headers', $tester->getDisplay());
        self::assertSame(0, $tester->getStatusCode());
    }

    private function captureConsoleOutput(\Closure $closure) : string
    {
        $output = new BufferedOutput();

        $io = new SymfonyStyle(new ArrayInput([]), $output);

        $closure($io);

        return $output->fetch();
    }
}
