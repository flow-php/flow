<?php

declare(strict_types=1);

namespace Flow\ParquetViewer\Tests\Integration;

use Flow\ParquetViewer\Parquet;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Console\Tester\ApplicationTester;

final class ReadMetadataTest extends TestCase
{
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

        self::assertStringContainsString(
            'not a valid parquet file',
            $tester->getDisplay()
        );
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

        self::assertStringContainsString('Metadata', $tester->getDisplay());
        self::assertStringContainsString('Row Groups', $tester->getDisplay());
        self::assertStringContainsString('Column Chunks', $tester->getDisplay());
        self::assertStringContainsString('Column Chunks Statistics', $tester->getDisplay());
        self::assertStringContainsString('Page Headers', $tester->getDisplay());
        self::assertSame(0, $tester->getStatusCode());
    }
}
