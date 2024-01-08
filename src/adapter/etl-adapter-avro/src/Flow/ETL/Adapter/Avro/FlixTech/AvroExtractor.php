<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Avro\FlixTech;

use function Flow\ETL\DSL\array_to_rows;
use Flow\ETL\Extractor;
use Flow\ETL\Extractor\FileExtractor;
use Flow\ETL\Extractor\Limitable;
use Flow\ETL\Extractor\LimitableExtractor;
use Flow\ETL\Extractor\PartitionFiltering;
use Flow\ETL\Extractor\PartitionsExtractor;
use Flow\ETL\Extractor\Signal;
use Flow\ETL\Filesystem\Path;
use Flow\ETL\Filesystem\Stream\Mode;
use Flow\ETL\FlowContext;

final class AvroExtractor implements Extractor, FileExtractor, LimitableExtractor, PartitionsExtractor
{
    use Limitable;
    use PartitionFiltering;

    public function __construct(
        private readonly Path $path
    ) {
        $this->resetLimit();
    }

    public function extract(FlowContext $context) : \Generator
    {
        $shouldPutInputIntoRows = $context->config->shouldPutInputIntoRows();

        foreach ($context->streams()->fs()->scan($this->path, $this->partitionFilter()) as $filePath) {
            $reader = new \AvroDataIOReader(
                new AvroResource(
                    $context->streams()->fs()->open(
                        $filePath,
                        Mode::READ_BINARY
                    )->resource()
                ),
                new \AvroIODatumReader(null, null),
            );

            $valueConverter = new ValueConverter(\json_decode($reader->metadata['avro.schema'], true));

            foreach ($reader->data() as $rowData) {
                $row = $valueConverter->convert($rowData);

                if ($shouldPutInputIntoRows) {
                    $row['_input_file_uri'] = $filePath->uri();
                }

                $signal = yield array_to_rows($row, $context->entryFactory(), $filePath->partitions());
                $this->countRow();

                if ($signal === Signal::STOP || $this->reachedLimit()) {
                    $context->streams()->close($this->path);

                    return;
                }
            }
        }

        $context->streams()->close($this->path);
    }

    public function source() : Path
    {
        return $this->path;
    }
}
