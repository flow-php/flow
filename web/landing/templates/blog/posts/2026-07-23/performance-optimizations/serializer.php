<?php

declare(strict_types=1);

namespace Flow\Serializer;

use Flow\ETL\Rows;
use Flow\Filesystem\DestinationStream;
use Flow\Filesystem\SourceStream;
use Flow\Serializer\Exception\SerializationException;

/**
 * @internal
 */
interface Serializer
{
    /**
     * @throws SerializationException
     */
    public function serialize(Rows $rows, DestinationStream $destination): void;

    /**
     * @throws SerializationException
     */
    public function unserialize(SourceStream $source): Rows;
}
