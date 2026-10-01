<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\XML;

use Flow\ETL\Column\Backend;
use Flow\ETL\Column\Php\ScalarColumn;
use Flow\ETL\Column\Php\XmlDocumentPhysical;
use Flow\ETL\Rows;
use Flow\ETL\Rows\RowsBuilder;
use Flow\ETL\Schema;
use Flow\ETL\Schema\Definition\XMLDefinition;
use Flow\Filesystem\SourceStream;
use Generator;

use function count;

final readonly class XMLNodeBatches
{
    /**
     * @param int<1, max> $batchSize
     * @param int<1, max> $bufferSize bytes read from the stream at a time
     */
    public function __construct(
        private XMLNodes $nodes,
        private int $batchSize,
        private int $bufferSize,
    ) {}

    /**
     * @return Generator<int, Rows>
     */
    public function documents(SourceStream $stream, Schema $body, Backend $backend): Generator
    {
        $batch = [];

        foreach ($this->nodes->documents($stream, $this->bufferSize) as $document) {
            $batch[] = ['node' => $document];

            if (count($batch) >= $this->batchSize) {
                yield (new RowsBuilder($body, $backend))
                    ->appendRows($batch)
                    ->finish();

                $batch = [];
            }
        }

        if ($batch !== []) {
            yield (new RowsBuilder($body, $backend))
                ->appendRows($batch)
                ->finish();
        }
    }

    /**
     * @return Generator<int, Rows>
     */
    public function physicals(SourceStream $stream, XMLDefinition $node, Schema $body, Backend $backend): Generator
    {
        $documents = new XmlDocumentPhysical();
        $batch = [];

        foreach ($this->nodes->texts($stream, $this->bufferSize) as $text) {
            $batch[] = $documents->physical($text);

            if (count($batch) >= $this->batchSize) {
                yield Rows::fromColumns(
                    $body,
                    ['node' => $backend->adopt($node, new ScalarColumn($node->type(), $documents, $batch, 0))],
                    count($batch),
                );

                $batch = [];
            }
        }

        if ($batch !== []) {
            yield Rows::fromColumns(
                $body,
                ['node' => $backend->adopt($node, new ScalarColumn($node->type(), $documents, $batch, 0))],
                count($batch),
            );
        }
    }

    /**
     * @return Generator<int, Rows>
     */
    public function strings(SourceStream $stream, Schema $body, Backend $backend): Generator
    {
        $batch = [];

        foreach ($this->nodes->texts($stream, $this->bufferSize) as $text) {
            $batch[] = ['node' => $text];

            if (count($batch) >= $this->batchSize) {
                yield (new RowsBuilder($body, $backend))
                    ->appendRows($batch)
                    ->finish();

                $batch = [];
            }
        }

        if ($batch !== []) {
            yield (new RowsBuilder($body, $backend))
                ->appendRows($batch)
                ->finish();
        }
    }
}
