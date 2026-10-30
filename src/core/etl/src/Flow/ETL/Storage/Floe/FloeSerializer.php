<?php

declare(strict_types=1);

namespace Flow\ETL\Storage\Floe;

use Flow\ETL\Foundation\Describe\Describable;
use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\RuntimeException;
use Flow\ETL\Storage\Serializer\Exception\SerializationException;
use Flow\ETL\Storage\Serializer\Serializer;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Column\Frame\FloeException;
use Flow\ETL\Values\Column\Frame\NoopCodec;
use Flow\ETL\Values\Rows;
use Flow\Filesystem\DestinationStream;
use Flow\Filesystem\SourceStream;
use SensitiveParameter;

use function Flow\Types\DSL\type_integer;
use function max;
use function sprintf;

final class FloeSerializer implements Describable, Serializer
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

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self(
            $rebuild->of($data['backend'] ?? null, Backend::class),
            max(1, type_integer()->assert($data['batch_size'] ?? null)),
        );
    }

    public function describe(): ?Description
    {
        return Description::compose(self::class, ['backend' => $this->backend, 'batch_size' => $this->batchSize]);
    }

    public function serialize(Rows $rows, DestinationStream $destination): void
    {
        try {
            // validation stays on until an upstream mechanism guarantees Rows match their schema
            $writer = FloeStreamWriter::create($destination, $rows->schema(), $this->backend);
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

            return Rows::concat($reader->schema(), $this->backend, ...$batches);
        } catch (FloeException|RuntimeException $e) {
            throw new SerializationException($e->getMessage(), 0, $e);
        } finally {
            $source->close();
        }
    }
}
