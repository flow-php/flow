<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet;

use Flow\ETL\Column\Backend;
use Flow\ETL\FlowPhpExtension;
use Flow\ETL\Schema;
use Flow\Parquet\Engine\RustParquetFileReader;
use Flow\Parquet\ParquetFile;
use Iterator;

final readonly class AdaptiveParquetOpenSource implements ParquetOpenSource
{
    private ParquetOpenSource $source;

    public function __construct(ParquetFile $file, ?FlowPhpExtension $extension = null)
    {
        $reader = $file->reader();

        $this->source = ($extension ?? FlowPhpExtension::detect())->available()
        && $reader instanceof RustParquetFileReader
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
