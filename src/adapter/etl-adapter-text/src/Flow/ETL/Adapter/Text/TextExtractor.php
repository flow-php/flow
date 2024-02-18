<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Text;

use function Flow\ETL\DSL\array_to_rows;
use Flow\ETL\Extractor;
use Flow\ETL\Extractor\FileExtractor;
use Flow\ETL\Extractor\Limitable;
use Flow\ETL\Extractor\LimitableExtractor;
use Flow\ETL\Extractor\PartitionFiltering;
use Flow\ETL\Extractor\PartitionsExtractor;
use Flow\ETL\Extractor\Signal;
use Flow\ETL\Filesystem\Path;
use Flow\ETL\FlowContext;

final class TextExtractor implements Extractor, FileExtractor, LimitableExtractor, PartitionsExtractor
{
    use Limitable;
    use PartitionFiltering;

    public function __construct(
        private readonly Path $path,
    ) {
        $this->resetLimit();
    }

    public function extract(FlowContext $context) : \Generator
    {
        $shouldPutInputIntoRows = $context->config->shouldPutInputIntoRows();

        foreach ($context->streams()->scan($this->path, $this->partitionFilter()) as $stream) {

            $rowData = \fgets($stream->resource());

            if ($rowData === false) {
                return;
            }

            while ($rowData !== false) {
                if ($shouldPutInputIntoRows) {
                    $row = [['text' => \rtrim($rowData), '_input_file_uri' => $stream->path()->uri()]];
                } else {
                    $row = [['text' => \rtrim($rowData)]];
                }

                $signal = yield array_to_rows($row, $context->entryFactory(), $stream->path()->partitions());

                $this->countRow();

                if ($signal === Signal::STOP || $this->reachedLimit()) {
                    $context->streams()->closeWriters($this->path);

                    return;
                }

                $rowData = \fgets($stream->resource());
            }

            $stream->close();
        }
    }

    public function source() : Path
    {
        return $this->path;
    }
}
