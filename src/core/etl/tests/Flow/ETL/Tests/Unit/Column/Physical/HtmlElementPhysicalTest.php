<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Column\Physical;

use Flow\ETL\Column\Physical\HtmlElementPhysical;
use Flow\ETL\Exception\InvalidArgumentException;
use PHPUnit\Framework\Attributes\RequiresPhp;
use PHPUnit\Framework\TestCase;

use function Flow\Types\DSL\type_html_element;
use function Flow\Types\DSL\type_string;

final class HtmlElementPhysicalTest extends TestCase
{
    #[RequiresPhp('>= 8.4.0')]
    public function test_the_markup(): void
    {
        $physical = new HtmlElementPhysical();
        $bytes = type_string()->assert($physical->toPhysical(type_html_element()->cast('<p>a</p>')));

        static::assertSame("\x01\x008\x00<p>a</p><p>a</p>", $bytes);
        static::assertSame('<p>a</p>', $physical->markup($bytes));
        static::assertSame('<p>a</p>', $physical->markup('<p>a</p>'));
        static::assertNull($physical->fromPhysicalAll([$bytes, null])[1]);
    }

    #[RequiresPhp('>= 8.4.0')]
    public function test_a_round_trip_keeps_the_parent_and_the_siblings(): void
    {
        $physical = new HtmlElementPhysical();
        $section = type_html_element()
            ->cast('<article><section><h1>Name</h1></section><span>01</span></article>')
            ->querySelector('section');

        static::assertNotNull($section);

        $element = type_html_element()->assert($physical->fromPhysical($physical->toPhysical($section)));

        static::assertSame(
            '<section><h1>Name</h1></section>',
            $physical->markup(type_string()->assert($physical->toPhysical($element))),
        );
        static::assertSame('ARTICLE', $element->parentElement?->tagName);
        static::assertNull($element->parentElement->parentElement);
        static::assertSame('01', $element->nextElementSibling?->textContent);
    }

    #[RequiresPhp('>= 8.4.0')]
    public function test_bare_element_markup_of_earlier_builds_still_loads(): void
    {
        $element = type_html_element()->assert((new HtmlElementPhysical())->fromPhysical(
            '<section><h1>Name</h1></section>',
        ));

        static::assertSame('Name', $element->firstElementChild?->textContent);
        static::assertSame('BODY', $element->parentElement?->tagName);
    }

    #[RequiresPhp('>= 8.4.0')]
    public function test_the_document_without_the_element_markup_still_loads(): void
    {
        $physical = new HtmlElementPhysical();
        $bytes = "\x000\x00<article><section><h1>Name</h1></section><span>01</span></article>";
        $element = type_html_element()->assert($physical->fromPhysical($bytes));

        static::assertSame('SECTION', $element->tagName);
        static::assertSame('ARTICLE', $element->parentElement?->tagName);
        static::assertSame('<section><h1>Name</h1></section>', $physical->markup($bytes));
    }

    #[RequiresPhp('>= 8.4.0')]
    public function test_a_detached_element_is_stored_as_its_markup(): void
    {
        $element = type_html_element()->cast('<p>a</p>');
        $element->remove();

        static::assertSame('<p>a</p>', (new HtmlElementPhysical())->toPhysical($element));
    }

    #[RequiresPhp('>= 8.4.0')]
    public function test_a_path_outside_the_document_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Floe failed to restore HTMLElement');

        (new HtmlElementPhysical())->fromPhysical("\x015\x000\x00<html><head></head><body></body></html>");
    }

    #[RequiresPhp('>= 8.4.0')]
    public function test_markup_without_an_element_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Floe failed to restore HTMLElement from "text"');

        (new HtmlElementPhysical())->fromPhysical('text');
    }

    #[RequiresPhp('< 8.4.0')]
    public function test_restoring_below_php84_throws(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('requires PHP 8.4+');

        (new HtmlElementPhysical())->fromPhysical('<p>x</p>');
    }
}
