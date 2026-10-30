<?php

declare(strict_types=1);

namespace Flow\ETL\Storage\Serializer;

use Flow\ETL\Storage\Serializer\Exception\SerializationException;
use Flow\ETL\Values\Rows;
use Flow\Filesystem\DestinationStream;
use Flow\Filesystem\SourceStream;

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
