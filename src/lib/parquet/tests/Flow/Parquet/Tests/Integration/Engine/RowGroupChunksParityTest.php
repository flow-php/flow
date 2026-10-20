<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Integration\Engine;

use Flow\Filesystem\Stream\MemorySourceStream;
use Flow\Filesystem\Stream\NativeLocalSourceStream;
use Flow\Parquet\Engine\RustParquetFileReader;
use Flow\Parquet\Filter\Evaluation\RowGroupZoneMaps;
use Flow\Parquet\Reader;
use Flow\Parquet\Tests\Context\FilterRows;
use Flow\Parquet\Tests\Context\MemoryParquetFile;
use PHPUnit\Framework\Attributes\Group;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

use function array_keys;
use function array_values;
use function extension_loaded;
use function Flow\Filesystem\DSL\path_real;
use function var_export;

#[Group('native-extension')]
final class RowGroupChunksParityTest extends TestCase
{
    protected function setUp(): void
    {
        if (!extension_loaded('arrow')) {
            self::markTestSkipped('arrow is not loaded');
        }
    }

    #[TestWith(['row_groups_php'])]
    #[TestWith(['row_groups_arrow'])]
    #[TestWith(['row_groups_duckdb'])]
    #[TestWith(['../../../IO/Fixtures/pagination_row_group_1kb_5k_rows.snappy'])]
    #[TestWith(['../../../IO/Fixtures/EdgeCases/datapage_v2.snappy'])]
    public function test_the_arrow_footer_slices_give_the_zone_maps_of_the_php_footer(string $fixture): void
    {
        $bytes = FilterRows::bytes($fixture);
        $file = MemoryParquetFile::read(Reader::php(), $bytes);
        $columns = array_values($file->schema()->columnsFlat());
        $php = $file->reader()->footer()->rowGroupChunks($columns);
        $rust = (new RustParquetFileReader(new MemorySourceStream($bytes)))
            ->footer()
            ->rowGroupChunks($columns);

        static::assertSame($php->rowCounts(), $rust->rowCounts());

        foreach (array_keys($php->rowCounts()) as $group) {
            foreach ($columns as $column) {
                static::assertSame($php->columnOrder($column), $rust->columnOrder($column));
                // var_export: a NaN bound never equals itself
                static::assertSame(
                    var_export((new RowGroupZoneMaps($php, $group))->zone($column), true),
                    var_export((new RowGroupZoneMaps($rust, $group))->zone($column), true),
                );
            }
        }
    }

    public function test_deprecated_bounds_of_a_legacy_file_stay_untrusted_through_the_arrow_footer(): void
    {
        $fixture = __DIR__ . '/../IO/Fixtures/EdgeCases/datapage_v2.snappy.parquet';
        $columns = array_values(Reader::php()->read($fixture)->schema()->columnsFlat());
        $chunks = (new RustParquetFileReader(NativeLocalSourceStream::open(path_real($fixture))))
            ->footer()
            ->rowGroupChunks($columns);

        foreach ($columns as $column) {
            static::assertNotNull($chunks->chunk(0, $column)->statistics()?->min($column));
            static::assertNull((new RowGroupZoneMaps($chunks, 0))->zone($column)?->min);
            static::assertNull((new RowGroupZoneMaps($chunks, 0))->zone($column)?->max);
        }
    }
}
