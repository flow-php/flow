<?php declare(strict_types=1);

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

        $this->assertStringContainsString(
            'not a valid parquet file',
            $tester->getDisplay()
        );
        $this->assertSame(1, $tester->getStatusCode());
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

        $this->assertStringContainsString("Metadata", $tester->getDisplay());
        $this->assertStringContainsString("Row Groups", $tester->getDisplay());
        $this->assertStringContainsString("Column Chunks", $tester->getDisplay());
        $this->assertStringContainsString("Column Chunks Statistics", $tester->getDisplay());
        $this->assertStringContainsString("Page Headers", $tester->getDisplay());
        $this->assertSame(0, $tester->getStatusCode());
    }
}
