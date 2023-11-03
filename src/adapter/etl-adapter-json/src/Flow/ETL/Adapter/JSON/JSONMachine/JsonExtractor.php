<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON\JSONMachine;

use Flow\ETL\Extractor\FileExtractor;
use Flow\ETL\Extractor\Limitable;
use Flow\ETL\Extractor\LimitableExtractor;
use function Flow\ETL\DSL\array_to_rows;
use Flow\ETL\Extractor;
use Flow\ETL\Filesystem\Path;
use Flow\ETL\Filesystem\Stream\Mode;
use Flow\ETL\FlowContext;
use JsonMachine\Items;
use JsonMachine\JsonDecoder\ExtJsonDecoder;

final class JsonExtractor implements Extractor, FileExtractor, LimitableExtractor
{
    use Limitable;

    public function __construct(
        private readonly Path $path,
        private readonly ?string $pointer = null,
    ) {
    }

    public function extract(FlowContext $context) : \Generator
    {
        $shouldPutInputIntoRows = $context->config->shouldPutInputIntoRows();

        foreach ($context->streams()->fs()->scan($this->path, $context->partitionFilter()) as $filePath) {
            /**
             * @var array|object $rowData
             */
            foreach (Items::fromStream($context->streams()->fs()->open($filePath, Mode::READ)->resource(), $this->readerOptions())->getIterator() as $rowData) {
                $rowData = (array) $rowData;

                if ($shouldPutInputIntoRows) {
                    $rowData['_input_file_uri'] = $filePath->uri();
                }

                yield array_to_rows($row, $context->entryFactory());
            }
        }

        $context->streams()->close($this->path);
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
