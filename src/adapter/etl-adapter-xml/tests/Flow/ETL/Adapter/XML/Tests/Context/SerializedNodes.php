<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\XML\Tests\Context;

use DOMDocument;
use Flow\ETL\Adapter\XML\XMLNodes;
use Flow\Filesystem\Stream\StringSourceStream;

use function array_map;
use function Flow\Filesystem\DSL\path;
use function iterator_to_array;

final class SerializedNodes
{
    /**
     * @param int<1, max> $bufferSize
     *
     * @return list<string>
     */
    public static function of(string $xmlNodePath, string $xml, int $bufferSize = 8192): array
    {
        return array_map(
            static fn(DOMDocument $document): string => (string) $document->saveXML($document->documentElement),
            iterator_to_array(
                (new XMLNodes($xmlNodePath))->of(
                    new StringSourceStream(path('memory://document.xml'), $xml),
                    $bufferSize,
                ),
                false,
            ),
        );
    }
}
