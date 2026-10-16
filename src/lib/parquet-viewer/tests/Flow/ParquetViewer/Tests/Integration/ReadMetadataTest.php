<?php

declare(strict_types=1);

namespace Flow\ParquetViewer\Tests\Integration;

use Flow\ETL\Tests\CommandOutputNormalizer;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\Tests\Context\TestParquetFile;
use Flow\Parquet\Writer;
use Flow\ParquetViewer\Parquet;
use Flow\ParquetViewer\Tests\Context\StyledOutput;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Console\Style\OutputStyle;
use Symfony\Component\Console\Tester\ApplicationTester;

use function realpath;

final class ReadMetadataTest extends TestCase
{
    use CommandOutputNormalizer;

    protected function tearDown(): void
    {
        TestParquetFile::remove($this);
    }

    public function test_reading_metadata_from_non_json_file(): void
    {
        $application = new Parquet();
        $application->setAutoExit(false);
        $application->setCatchExceptions(false);

        $path = realpath(__DIR__ . '/../../Fixtures/flow.json');

        $tester = new ApplicationTester($application);
        $tester->run([
            'command' => 'read:metadata',
            'file' => $path,
        ]);

        $expected = StyledOutput::captured(static fn(OutputStyle $io) => $io->error(
            "File \"{$path}\" is not a valid parquet file",
        ));

        self::assertCommandOutputContains(self::normalizeCommandOutput($expected), $tester->getDisplay());
        static::assertSame(1, $tester->getStatusCode());
    }

    public function test_reading_metadata_from_parquet_file(): void
    {
        $application = new Parquet();
        $application->setAutoExit(false);
        $application->setCatchExceptions(false);

        $path = realpath(__DIR__ . '/../../Fixtures/flow.parquet');

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
        static::assertSame(0, $tester->getStatusCode());
    }

    public function test_decimal_statistics_are_shown_as_decimals(): void
    {
        $path = TestParquetFile::path($this);
        (new Writer())->write($path, Schema::with(FlatColumn::decimal('amount', 10, 2)), [
            ['amount' => -3.5],
            ['amount' => 1.25],
        ]);
        $application = new Parquet();
        $application->setAutoExit(false);
        $application->setCatchExceptions(false);
        $tester = new ApplicationTester($application);

        $tester->run(['command' => 'read:metadata', 'file' => $path, '--statistics' => 1]);

        self::assertCommandOutputContains('-3.5', $tester->getDisplay());
        self::assertCommandOutputContains('1.25', $tester->getDisplay());
        static::assertSame(0, $tester->getStatusCode());
    }

    public function test_uuid_statistics_are_shown_as_uuids(): void
    {
        $path = TestParquetFile::path($this);
        (new Writer())->write($path, Schema::with(FlatColumn::uuid('id')), [
            ['id' => '0f8fad5b-d9cb-469f-a165-70867728950e'],
            ['id' => '7c9e6679-7425-40de-944b-e07fc1f90ae7'],
        ]);
        $application = new Parquet();
        $application->setAutoExit(false);
        $application->setCatchExceptions(false);
        $tester = new ApplicationTester($application);

        $tester->run(['command' => 'read:metadata', 'file' => $path, '--statistics' => 1]);

        self::assertCommandOutputContains('0f8fad5b-d9cb-469f-a165-70867728950e', $tester->getDisplay());
        self::assertCommandOutputContains('7c9e6679-7425-40de-944b-e07fc1f90ae7', $tester->getDisplay());
        static::assertSame(0, $tester->getStatusCode());
    }
}
