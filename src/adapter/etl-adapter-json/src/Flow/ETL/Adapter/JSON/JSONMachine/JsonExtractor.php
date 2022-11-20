<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON\JSONMachine;

use Flow\ETL\Extractor;
use Flow\ETL\Filesystem\Path;
use Flow\ETL\Filesystem\Stream\Mode;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use JsonMachine\Items;

final class JsonExtractor implements Extractor
{
    public function __construct(
        private readonly Path $path,
        private readonly int $rowsInBatch = 1000,
        private readonly string $rowEntryName = 'row',
        private readonly ?string $pointer = null
    ) {
    }

    public function extract(FlowContext $context) : \Generator
    {
        $rows = new Rows();

        foreach ($context->streams()->fs()->scan($this->path, $context->partitionFilter()) as $filePath) {
            /**
             * @var array|object $row
             */
            foreach (Items::fromStream($context->streams()->fs()->open($filePath, Mode::READ)->resource(), $this->readerOptions())->getIterator() as $row) {
                $rows = $rows->add(Row::create(new Row\Entry\ArrayEntry($this->rowEntryName, (array) $row)));

                if ($rows->count() >= $this->rowsInBatch) {
                    yield $rows;

                    $rows = new Rows();
                }
            }

            if ($rows->count()) {
                yield $rows;
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
