<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Extractor;

use Flow\ETL\Extractor\File\FileScan;
use Flow\ETL\Tests\Context\ExtractedRows;
use Flow\ETL\Tests\FlowIntegrationTestCase;
use PHPUnit\Framework\Attributes\TestWith;

use function Flow\ETL\Adapter\CSV\from_csv;
use function Flow\ETL\Adapter\CSV\to_csv;
use function Flow\ETL\Adapter\Excel\DSL\from_excel;
use function Flow\ETL\Adapter\Excel\DSL\to_excel;
use function Flow\ETL\Adapter\JSON\from_json;
use function Flow\ETL\Adapter\JSON\to_json;
use function Flow\ETL\Adapter\Parquet\from_parquet;
use function Flow\ETL\Adapter\Parquet\to_parquet;
use function Flow\ETL\Adapter\Text\from_text;
use function Flow\ETL\Adapter\Text\to_text;
use function Flow\ETL\Adapter\XML\from_xml;
use function Flow\ETL\Adapter\XML\to_xml;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\files;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\from_path_partitions;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\Filesystem\DSL\path;
use function Flow\Floe\DSL\from_floe;
use function Flow\Floe\DSL\to_floe;

/**
 * Avro is left out: its extractor's constructor throws unconditionally.
 */
final class FileExtractorScanTest extends FlowIntegrationTestCase
{
    #[TestWith(['csv'])]
    #[TestWith(['json'])]
    #[TestWith(['text'])]
    #[TestWith(['xml'])]
    #[TestWith(['excel'])]
    #[TestWith(['parquet'])]
    #[TestWith(['floe'])]
    #[TestWith(['files'])]
    #[TestWith(['path_partitions'])]
    public function test_extract_with_a_limit_reads_what_scan_with_that_limit_reads(string $family): void
    {
        $directory = $this->cacheDir->path() . '/scan-' . $family;
        $uri = path($directory . '/rows.' . $family);
        $xlsx = path($directory . '/rows.xlsx');

        [$sink, $extractor] = match ($family) {
            'csv' => [to_csv($uri), from_csv($uri)],
            'json' => [to_json($uri), from_json($uri)],
            'text' => [to_text($uri), from_text($uri)],
            'xml' => [to_xml($uri), from_xml($uri)],
            'excel' => [to_excel($xlsx), from_excel($xlsx)],
            'parquet' => [to_parquet($uri), from_parquet($uri)],
            'floe' => [to_floe($uri), from_floe($uri)],
            'files' => [to_csv($uri), files($directory . '/*')],
            default => [to_csv(path($directory . '/id=a/rows.csv')), from_path_partitions($directory . '/*/*.csv')],
        };

        df()
            ->read(from_array([['id' => 'a'], ['id' => 'b'], ['id' => 'c']]))
            ->write($sink)
            ->run();

        $context = flow_context();
        $scanned = rows(schema());

        foreach ($extractor->scan($context, new FileScan(limit: 2)) as $batch) {
            $scanned = $scanned->isEmpty() ? $batch : $scanned->concat($context->backend(), $batch);
        }

        static::assertNotSame([], $scanned->toArray());
        static::assertEquals(ExtractedRows::of($extractor, $context, new FileScan(2))->toArray(), $scanned->toArray());
    }
}
