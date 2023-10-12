<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON\JSONMachine;

use function Flow\ETL\DSL\array_to_rows;
use Flow\ETL\Extractor;
use Flow\ETL\Filesystem\Path;
use Flow\ETL\Filesystem\Stream\Mode;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use JsonMachine\Items;

final class JsonExtractor implements Extractor
{
    public function __construct(
        private readonly Path $path,
        private readonly int $rowsInBatch = 1000,
        private readonly ?string $pointer = null,
        private readonly Row\EntryFactory $entryFactory = new Row\Factory\NativeEntryFactory()
    ) {
    }

    public function extract(FlowContext $context) : \Generator
    {
        $rows = [];

        foreach ($context->streams()->fs()->scan($this->path, $context->partitionFilter()) as $filePath) {
            /**
             * @var array|object $row
             */
            foreach (Items::fromStream($context->streams()->fs()->open($filePath, Mode::READ)->resource(), $this->readerOptions())->getIterator() as $row) {
                if ($context->config->shouldPutInputIntoRows()) {
                    $rows[] = \array_merge((array) $row, ['_input_file_uri' => $filePath->uri()]);
                } else {
                    $rows[] = (array) $row;
                }

                if (\count($rows) >= $this->rowsInBatch) {
                    yield array_to_rows($rows, $this->entryFactory);

                    $rows = [];
                }
            }

            if ([] !== $rows) {
                yield array_to_rows($rows, $this->entryFactory);
            }
        }
    }

    /**
     * @return array{pointer?: string}
     */
    private function readerOptions() : array
    {
        $options = [];

        if ($this->pointer !== null) {
            $options['pointer'] = $this->pointer;
        }

        return $options;
    }
}
