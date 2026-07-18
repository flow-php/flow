<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\Rows;
use Flow\Filesystem\DestinationStream;
use Flow\Filesystem\SourceStream;
use Flow\Floe\FloeSerializer;
use Flow\Serializer\Serializer;

use function Flow\Serializer\DSL\serialize_to_string;
use function Flow\Serializer\DSL\unserialize_from_string;

final class SpySerializer implements Serializer
{
    /**
     * @var list<string>
     */
    public array $serialized = [];

    /**
     * @var list<string>
     */
    public array $unserialized = [];

    public function __construct(
        private readonly Serializer $inner = new FloeSerializer(),
    ) {}

    public function serialize(Rows $rows, DestinationStream $destination): void
    {
        $payload = serialize_to_string($this->inner, $rows);
        $this->serialized[] = $payload;

        $destination->append($payload);
        $destination->close();
    }

    public function unserialize(SourceStream $source): Rows
    {
        $payload = $source->content();
        $this->unserialized[] = $payload;
        $source->close();

        return unserialize_from_string($this->inner, $payload);
    }
}
