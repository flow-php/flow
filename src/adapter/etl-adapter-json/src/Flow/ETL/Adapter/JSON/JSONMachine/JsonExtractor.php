<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON\JSONMachine;

use function Flow\ETL\DSL\array_to_rows;
use Flow\ETL\Extractor;
use Flow\ETL\Filesystem\Path;
use Flow\ETL\Filesystem\Stream\Mode;
use Flow\ETL\FlowContext;
use JsonMachine\Items;
use JsonMachine\JsonDecoder\ExtJsonDecoder;

final class JsonExtractor implements Extractor, Extractor\FileExtractor
{
    public function __construct(
        private readonly Path $path,
        private readonly int $rowsInBatch = 1000,
        private readonly ?string $pointer = null,
    ) {
    }

    public function extract(FlowContext $context) : \Generator
    {
        $rows = [];

        $shouldPutInputIntoRows = $context->config->shouldPutInputIntoRows();

        foreach ($context->streams()->fs()->scan($this->path, $context->partitionFilter()) as $filePath) {
            /**
             * @var array|object $row
             */
            foreach (Items::fromStream($context->streams()->fs()->open($filePath, Mode::READ)->resource(), $this->readerOptions())->getIterator() as $row) {
                $row = (array) $row;

                if ($shouldPutInputIntoRows) {
                    $row['_input_file_uri'] = $filePath->uri();
                }

                $rows[] = $row;

                if (\count($rows) >= $this->rowsInBatch) {
                    yield array_to_rows($rows, $context->entryFactory());

                    $rows = [];
                }
            }

            if ([] !== $rows) {
                yield array_to_rows($rows, $context->entryFactory());
            }
        }
    }

    public function source() : Path
    {
        return $this->path;
    }

    /**
     * @return array{pointer?: string, decoder: ExtJsonDecoder}
     */
    private function readerOptions() : array
    {
        $options = [
            'decoder' => new ExtJsonDecoder(true),
        ];

        if ($this->pointer !== null) {
            $options['pointer'] = $this->pointer;
        }

        return $options;
    }
}
