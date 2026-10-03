<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Column\Physical;

use DOMDocument;
use DOMElement;
use Flow\ETL\Column\Physical\ElementPosition;
use PHPUnit\Framework\TestCase;

use function Flow\Types\DSL\type_instance_of;

final class ElementPositionTest extends TestCase
{
    public function test_the_path_counts_element_children_from_the_document_element(): void
    {
        $document = new DOMDocument();
        $document->loadXML('<root><a/>text<b><c/><d/></b></root>');
        $position = new ElementPosition();
        $root = type_instance_of(DOMElement::class)->assert($document->documentElement);
        $d = type_instance_of(DOMElement::class)->assert($root->lastElementChild?->lastElementChild);

        static::assertSame('', $position->path($root));
        static::assertSame('1/1', $position->path($d));
        static::assertSame($d, $position->element($root, '1/1'));
        static::assertSame($root, $position->element($root, ''));
        static::assertNull($position->element($root, '2'));
        static::assertNull($position->element($root, '0/0'));
    }

    public function test_an_element_outside_the_document_tree_has_no_path(): void
    {
        $document = new DOMDocument();
        $document->loadXML('<root/>');

        static::assertNull((new ElementPosition())->path(type_instance_of(
            DOMElement::class,
        )->assert($document->createElement('detached'))));
    }

    public function test_the_physical_splits_into_the_path_the_markup_and_the_document(): void
    {
        $position = new ElementPosition();
        $physical = $position->physical('0/2', "<b>\0</b>", "<root>\0</root>");

        static::assertSame("\x010/2\x008\x00<b>\0</b><root>\0</root>", $physical);
        static::assertSame('0/2', $position->pathOf($physical));
        static::assertSame("<b>\0</b>", $position->markupOf($physical));
        static::assertSame("<root>\0</root>", $position->documentOf($physical));
    }

    public function test_the_document_without_the_markup_splits_into_the_path_and_the_document(): void
    {
        $position = new ElementPosition();

        static::assertSame('0/2', $position->pathOf("\x000/2\x00<root/>"));
        static::assertNull($position->markupOf("\x000/2\x00<root/>"));
        static::assertSame('<root/>', $position->documentOf("\x000/2\x00<root/>"));
    }

    public function test_bare_markup_has_no_position(): void
    {
        static::assertNull((new ElementPosition())->pathOf('<root/>'));
        static::assertNull((new ElementPosition())->markupOf('<root/>'));
    }
}
