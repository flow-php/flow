<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet;

use Flow\ETL\Column\Backend;
use Flow\ETL\Schema;
use Flow\Parquet\Engine\RustParquetFileReader;
use Flow\Parquet\ParquetFile;
use Iterator;

use function extension_loaded;

final readonly class AdaptiveParquetOpenSource implements ParquetOpenSource
{
    private ParquetOpenSource $source;

    public function __construct(ParquetFile $file)
    {
        $reader = $file->reader();

        $this->source = extension_loaded('flow_php') && $reader instanceof RustParquetFileReader
            ? new RustParquetOpenSource($reader)
            : new PhpParquetOpenSource($file);
    }

    public function batches(Schema $schema, int $batchSize, ?int $offset, ?int $limit, Backend $backend): Iterator
    {
        return $this->source->batches($schema, $batchSize, $offset, $limit, $backend);
    }

    public function close(): void
    {
        $this->source->close();
    }
}
