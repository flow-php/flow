<?php

declare(strict_types=1);

namespace Flow\Floe;

use Flow\ETL\Row\Hydrator;
use Flow\ETL\Rows;
use Flow\Filesystem\DestinationStream;
use Flow\Filesystem\SourceStream;
use Flow\Floe\Codec\NoopCodec;
use Flow\Floe\Exception\ExtensionException;
use Flow\Floe\Exception\FloeException;
use Flow\Serializer\Exception\SerializationException;
use Flow\Serializer\Serializer;

use function count;
use function sprintf;

final class FloeSerializer implements Serializer
{
    private const int CHUNK_SIZE = 65_536;

    /**
     * @param int<1, max> $batchSize
     * @param null|Hydrator $hydrator null uses the adaptive hydrator
     */
    public function __construct(
        private readonly int $batchSize = 1000,
        private readonly ?Hydrator $hydrator = null,
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
            $writer = new FloeStreamWriter($rows->schema(), new Options(), hydrator: $this->hydrator);
            $writer->create($destination);
            $writer->write($rows);
            $writer->close();
        } catch (FloeException|ExtensionException $e) {
            throw new SerializationException($e->getMessage(), 0, $e);
        }
    }

    public function unserialize(SourceStream $source): Rows
    {
        try {
            $reader = new FloeStreamReader($source, new NoopCodec(), self::CHUNK_SIZE, $this->hydrator);

            $footer = $reader->footer();

            $rows = [];

            foreach ($reader->rows($this->batchSize) as $batch) {
                foreach ($batch->all() as $row) {
                    $rows[] = $row;
                }
            }

            if (count($rows) !== $footer->totalRows) {
                throw new FloeException(sprintf(
                    'Floe payload is corrupted, decoded %d of %d rows',
                    count($rows),
                    $footer->totalRows,
                ));
            }

            // every row came out of a reader batch already conformed to this schema
            return Rows::trusted($reader->schema(), $rows);
        } catch (FloeException|ExtensionException $e) {
            throw new SerializationException($e->getMessage(), 0, $e);
        } finally {
            $source->close();
        }
    }
}
