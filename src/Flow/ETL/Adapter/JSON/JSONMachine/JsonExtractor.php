<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON\JSONMachine;

use Flow\ETL\Extractor;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Stream\FileStream;
use Flow\ETL\Stream\Handler;
use Flow\ETL\Stream\LocalFile;
use Flow\ETL\Stream\Mode;
use JsonMachine\Items;

/**
 * @psalm-immutable
 */
final class JsonExtractor implements Extractor
{
    private FileStream $stream;

    public function __construct(
        FileStream|string $stream,
        private readonly int $rowsInBatch = 1000,
        private readonly string $rowEntryName = 'row'
    ) {
        if (\is_string($stream)) {
            $this->stream = new LocalFile($stream);
        } else {
            $this->stream = $stream;
        }
    }

    public function extract() : \Generator
    {
        $rows = new Rows();

        /**
         * @psalm-suppress ImpureMethodCall
         *
         * @var array|object $row
         */
        foreach ($this->items()->getIterator() as $row) {
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

    /**
     * @psalm-suppress ImpureMethodCall
     */
    private function items() : Items
    {
        return Items::fromStream(Handler::file()->open($this->stream, Mode::READ));
    }
}
