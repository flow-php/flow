<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use ArrayObject;
use DOMDocument;

/**
 * An XML document that records every canonicalisation - the comparison xml values go through.
 */
final class CountingDOMDocument extends DOMDocument
{
    /**
     * @param ArrayObject<int, string> $calls
     */
    public function __construct(
        public readonly ArrayObject $calls,
    ) {
        parent::__construct();
        $this->loadXML('<a/>');
    }

    public function C14N(
        bool $exclusive = false,
        bool $withComments = false,
        ?array $xpath = null,
        ?array $nsPrefixes = null,
    ): string|false {
        $this->calls->append('C14N');

        return parent::C14N($exclusive, $withComments, $xpath, $nsPrefixes);
    }
}
