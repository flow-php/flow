<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Integration\Engine;

use Flow\Filesystem\Stream\NativeLocalSourceStream;
use Flow\Parquet\Engine\RustParquetFileReader;
use Flow\Parquet\Filter\Evaluation\RowGroupZoneMaps;
use Flow\Parquet\Reader;
use PHPUnit\Framework\Attributes\Group;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

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

    #[TestWith(['Engine/Fixtures/Filters/row_groups_php.parquet'])]
    #[TestWith(['Engine/Fixtures/Filters/row_groups_arrow.parquet'])]
    #[TestWith(['Engine/Fixtures/Filters/row_groups_duckdb.parquet'])]
    #[TestWith(['IO/Fixtures/pagination_row_group_1kb_5k_rows.snappy.parquet'])]
    #[TestWith(['IO/Fixtures/EdgeCases/datapage_v2.snappy.parquet'])]
    public function test_the_arrow_footer_slices_give_the_zone_maps_of_the_php_footer(string $fixture): void
    {
        $file = Reader::php()->read(__DIR__ . '/../' . $fixture);
        $columns = array_values($file->schema()->columnsFlat());
        $php = $file->reader()->footer()->rowGroupChunks($columns);
        $rust = (new RustParquetFileReader(NativeLocalSourceStream::open(path_real(__DIR__ . '/../' . $fixture))))
            ->footer()
            ->rowGroupChunks($columns);

        static::assertSame($php->count(), $rust->count());

        for ($group = 0; $group < $php->count(); $group++) {
            static::assertSame($php->rowCount($group), $rust->rowCount($group));

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
