<?php

declare(strict_types=1);

namespace Flow\Floe;

use Flow\ETL\Column\Backend;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Rows;
use Flow\ETL\Rows\RowsBuilder;
use Flow\Filesystem\DestinationStream;
use Flow\Filesystem\SourceStream;
use Flow\Floe\Codec\NoopCodec;
use Flow\Floe\Exception\FloeException;
use Flow\Serializer\Exception\SerializationException;
use Flow\Serializer\Serializer;

use function array_slice;
use function sprintf;

final class FloeSerializer implements Serializer
{
    private const int CHUNK_SIZE = 65_536;

    /**
     * @param int<1, max> $batchSize
     */
    public function __construct(
        private readonly Backend $backend,
        private readonly int $batchSize = 1000,
    ) {
        // @mago-ignore analysis:impossible-condition,redundant-comparison
        if ($this->batchSize < 1) {
            throw new FloeException('Serializer batch size must be at least 1');
        }
    }

    public function serialize(Rows $rows, DestinationStream $destination): void
    {
        try {
            // validation stays on until an upstream mechanism guarantees Rows match their schema
            $writer = new FloeStreamWriter($rows->schema(), $this->backend, new Options());
            $writer->create($destination);
            $writer->write($rows);
            $writer->close();
        } catch (FloeException|RuntimeException $e) {
            throw new SerializationException($e->getMessage(), 0, $e);
        }
    }

    public function unserialize(SourceStream $source): Rows
    {
        try {
            $reader = new FloeStreamReader($source, new NoopCodec(), self::CHUNK_SIZE, $this->backend);

            $footer = $reader->footer();

            $batches = [];
            $decoded = 0;

            foreach ($reader->rows($this->batchSize) as $batch) {
                $batches[] = $batch;
                $decoded += $batch->count();
            }

            if ($decoded !== $footer->statistics->rows) {
                throw new FloeException(sprintf(
                    'Floe payload is corrupted, decoded %d of %d rows',
                    $decoded,
                    $footer->statistics->rows,
                ));
            }

            return $batches === []
                ? (new RowsBuilder($reader->schema(), $this->backend))->finish()
                : $batches[0]->concat($this->backend, ...array_slice($batches, 1));
        } catch (FloeException|RuntimeException $e) {
            throw new SerializationException($e->getMessage(), 0, $e);
        } finally {
            $source->close();
        }
    }
}
