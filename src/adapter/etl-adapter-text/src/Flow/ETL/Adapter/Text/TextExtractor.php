<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Text;

use function Flow\ETL\DSL\array_to_rows;
use Flow\ETL\Extractor;
use Flow\ETL\Filesystem\Path;
use Flow\ETL\Filesystem\Stream\Mode;
use Flow\ETL\FlowContext;

final class TextExtractor implements Extractor, Extractor\FileExtractor
{
    public function __construct(
        private readonly Path $path,
    ) {
    }

    public function extract(FlowContext $context) : \Generator
    {
        $shouldPutInputIntoRows = $context->config->shouldPutInputIntoRows();

        foreach ($context->streams()->fs()->scan($this->path, $context->partitionFilter()) as $filePath) {
            $fileStream = $context->streams()->fs()->open($filePath, Mode::READ);

            $rowData = \fgets($fileStream->resource());

            if ($rowData === false) {
                return;
            }

            while ($rowData !== false) {
                if ($shouldPutInputIntoRows) {
                    $row = [['text' => \rtrim($rowData), '_input_file_uri' => $filePath->uri()]];
                } else {
                    $row = [['text' => \rtrim($rowData)]];
                }

                yield array_to_rows($row, $context->entryFactory());

                $rowData = \fgets($fileStream->resource());
            }
        }

        $context->streams()->close($this->path);
    }

    public function source() : Path
    {
        return $this->path;
    }
}
