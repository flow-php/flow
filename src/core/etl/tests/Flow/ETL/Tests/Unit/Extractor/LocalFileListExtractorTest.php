<?php declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Extractor;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\local_files;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Extractor\Signal;
use PHPUnit\Framework\TestCase;

final class LocalFileListExtractorTest extends TestCase
{
    protected function setUp() : void
    {
        if (!\in_array('flow-remote-file', \stream_get_wrappers(), true)) {
            \stream_wrapper_register('flow-remote-file', self::class);
        }
    }

    protected function tearDown() : void
    {
        if (\in_array('flow-remote-file', \stream_get_wrappers(), true)) {
            \stream_wrapper_unregister('flow-remote-file');
        }
    }

    public function test_extracting_files_from_directory() : void
    {
        $extractor = local_files(__DIR__ . '/Fixtures/FileListExtractor');

        $totalRows = 0;

        foreach ($extractor->extract(flow_context()) as $rows) {
            $this->assertCount(1, $rows);
            $totalRows += $rows->count();
        }

        $this->assertEquals(6, $totalRows);
    }

    public function test_extracting_files_from_directory_after_getting_stop_signal() : void
    {
        $extractor = local_files(__DIR__ . '/Fixtures/FileListExtractor', true);
        $generator = $extractor->extract(flow_context());
        $totalRows = 0;

        foreach ($generator as $rows) {
            $this->assertCount(1, $rows);
            $totalRows += $rows->count();
            $generator->send(Signal::STOP);
        }

        $this->assertEquals(1, $totalRows);
    }

    public function test_extracting_files_from_directory_recursive() : void
    {
        $extractor = local_files(__DIR__ . '/Fixtures/FileListExtractor', true);

        $totalRows = 0;

        foreach ($extractor->extract(flow_context()) as $rows) {
            $this->assertCount(1, $rows);
            $totalRows += $rows->count();
        }

        $this->assertEquals(7, $totalRows);
    }

    public function test_extracting_files_from_directory_with_limit() : void
    {
        $extractor = local_files(__DIR__ . '/Fixtures/FileListExtractor', true);
        $extractor->changeLimit(2);

        $totalRows = 0;

        foreach ($extractor->extract(flow_context()) as $rows) {
            $this->assertCount(1, $rows);
            $totalRows += $rows->count();
        }

        $this->assertEquals(2, $totalRows);
    }

    public function test_extracting_from_directory_with_pattern() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('LocalFileListExtractor does not support glob paths');
        local_files(__DIR__ . '/Fixtures/FileListExtractor/*');
    }

    public function test_extracting_from_remote_directory() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Path must point to a local directory');
        local_files('flow-remote-file://bucket-name/path/to/file');
    }
}
