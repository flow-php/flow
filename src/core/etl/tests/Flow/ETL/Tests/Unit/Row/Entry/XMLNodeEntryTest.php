<?php declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Entry;

use Flow\ETL\Row\Entry\XMLNodeEntry;
use PHPUnit\Framework\TestCase;

final class XMLNodeEntryTest extends TestCase
{
    public function test_serialization() : void
    {
        $domDocument = new \DOMDocument();

        $entry = new XMLNodeEntry('node', $domDocument->createElement('testElement', 'This is a test'));

        $serialized = \serialize($entry);
        $unserialized = \unserialize($serialized);

        $this->assertTrue($entry->isEqual($unserialized));
    }
}
