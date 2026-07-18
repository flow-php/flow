<?php

declare(strict_types=1);

namespace Flow\Serializer\DSL;

use Flow\ETL\Attribute\DocumentationDSL;
use Flow\ETL\Attribute\Module;
use Flow\ETL\Attribute\Type as DSLType;
use Flow\ETL\Rows;
use Flow\Filesystem\Stream\StringDestinationStream;
use Flow\Filesystem\Stream\StringSourceStream;
use Flow\Serializer\Serializer;

use function Flow\Filesystem\DSL\path;

#[DocumentationDSL(module: Module::CORE, type: DSLType::HELPER)]
function serialize_to_string(Serializer $serializer, Rows $rows): string
{
    $destination = new StringDestinationStream(path('memory://serialized'));
    $serializer->serialize($rows, $destination);

    return $destination->content();
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::HELPER)]
function unserialize_from_string(Serializer $serializer, string $payload): Rows
{
    return $serializer->unserialize(new StringSourceStream(path('memory://serialized'), $payload));
}
