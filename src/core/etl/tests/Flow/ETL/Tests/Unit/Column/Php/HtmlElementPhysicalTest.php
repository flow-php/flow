<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Column\Php;

use Flow\ETL\Column\Php\HtmlElementPhysical;
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
        $markup = type_string()->assert($physical->toPhysical(type_html_element()->cast('<p>a</p>')));

        static::assertSame('<p>a</p>', $markup);
        static::assertSame('<p>a</p>', $physical->toPhysical($physical->fromPhysical('<p>a</p>')));
        static::assertNull($physical->fromPhysicalAll(['<p>a</p>', null])[1]);
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
