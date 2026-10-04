<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Mother;

use DOMDocument;

final class XmlMother
{
    public static function document(string $xml): DOMDocument
    {
        $document = new DOMDocument();
        $document->loadXML($xml);

        return $document;
    }
}
