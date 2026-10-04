<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\XML;

use Flow\ETL\Column\Backend;
use Flow\ETL\Column\Physical\XmlDocumentPhysical;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\ETL\Schema\Definition;
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
        $node = $body->get('node');
        $nodes = [];

        foreach ($this->nodes->documents($stream, $this->bufferSize) as $document) {
            $nodes[] = $document;

            if (count($nodes) >= $this->batchSize) {
                yield $this->batch($node, $nodes, $body, $backend);

                $nodes = [];
            }
        }

        if ($nodes !== []) {
            yield $this->batch($node, $nodes, $body, $backend);
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
                $builder = $backend->builder($node);
                $builder->appendPhysicals($batch, 0);

                yield Rows::fromColumns(new Schema($node), ['node' => $builder->finish()], count($batch))->matchTo(
                    $body,
                    $backend,
                );

                $batch = [];
            }
        }

        if ($batch !== []) {
            $builder = $backend->builder($node);
            $builder->appendPhysicals($batch, 0);

            yield Rows::fromColumns(new Schema($node), ['node' => $builder->finish()], count($batch))->matchTo(
                $body,
                $backend,
            );
        }
    }

    /**
     * @return Generator<int, Rows>
     */
    public function strings(SourceStream $stream, Schema $body, Backend $backend): Generator
    {
        $node = $body->get('node');
        $nodes = [];

        foreach ($this->nodes->texts($stream, $this->bufferSize) as $text) {
            $nodes[] = $text;

            if (count($nodes) >= $this->batchSize) {
                yield $this->batch($node, $nodes, $body, $backend);

                $nodes = [];
            }
        }

        if ($nodes !== []) {
            yield $this->batch($node, $nodes, $body, $backend);
        }
    }

    /**
     * The node column built from $nodes; every other column $body declares is padded as matchTo() pads it.
     *
     * @param Definition<mixed> $node
     * @param non-empty-list<mixed> $nodes
     */
    public function batch(Definition $node, array $nodes, Schema $body, Backend $backend): Rows
    {
        $builder = $backend->builder($node);
        $builder->appendMany($nodes);

        return Rows::fromColumns(new Schema($node), ['node' => $builder->finish()], count($nodes))->matchTo(
            $body,
            $backend,
        );
    }
}
