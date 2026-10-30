<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\IO\Extractor;

use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\RustIterator;
use Flow\ETL\Values\Schema;
use Flow\Parquet\Engine\RustParquetFileReader;
use Flow\Parquet\Reader\ReadPlan;
use RuntimeException;

use function extension_loaded;

if (extension_loaded('flow_php')) {
    return;
}

final class RustParquetOpenSource implements ParquetOpenSource
{
    public function __construct(RustParquetFileReader $file)
    {
        throw new RuntimeException('flow_php extension is not loaded');
    }

    /**
     * @throws \Flow\ETL\Foundation\Exception\InvalidArgumentException for a column whose type stores something other than the
     *                                                      file's column reads as, before any batch
     *
     * @return RustIterator<Rows>
     */
    public function batches(Schema $schema, int $batchSize, ReadPlan $plan, Backend $backend): RustIterator
    {
        throw new RuntimeException('flow_php extension is not loaded');
    }

    public function close(): void
    {
        throw new RuntimeException('flow_php extension is not loaded');
    }
}
