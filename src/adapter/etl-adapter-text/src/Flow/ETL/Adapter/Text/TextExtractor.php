<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Text;

use Flow\ETL\Extractor;
use Flow\ETL\Extractor\FileExtractor;
use Flow\ETL\Extractor\Limitable;
use Flow\ETL\Extractor\LimitableExtractor;
use Flow\ETL\Extractor\PathFiltering;
use Flow\ETL\Extractor\Signal;
use Flow\ETL\FlowContext;
use Flow\Filesystem\Path;
use Generator;

use function Flow\ETL\DSL\array_to_rows;
use function rtrim;

final class TextExtractor implements Extractor, FileExtractor, LimitableExtractor
{
    use Limitable;
    use PathFiltering;

    public function __construct(
        private readonly Path $path,
    ) {
        $this->resetLimit();
    }

    public function extract(FlowContext $context): Generator
    {
        $shouldPutInputIntoRows = $context->config->shouldPutInputIntoRows();

        foreach ($context->streams()->list($this->path, $this->filter()) as $stream) {
            $uri = $stream->path()->uri();

            foreach ($stream->readLines() as $rowData) {
                if ($shouldPutInputIntoRows) {
                    $row = [['text' => rtrim($rowData), '_input_file_uri' => $uri]];
                } else {
                    $row = [['text' => rtrim($rowData)]];
                }

                $signal = yield array_to_rows($row, $context->entryFactory(), $stream->path()->partitions());

                $this->incrementReturnedRows();

                if ($signal === Signal::STOP || $this->reachedLimit()) {
                    $context->streams()->closeStreams($this->path);

                    return;
                }
            }

            $stream->close();
        }
    }

    public function source(): Path
    {
        return $this->path;
    }
}
