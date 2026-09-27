<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Column\Php;

use Flow\ETL\Column\Php\HtmlElementPhysical;
use PHPUnit\Framework\Attributes\RequiresPhp;
use PHPUnit\Framework\TestCase;

use function Flow\Types\DSL\type_html_element;
use function Flow\Types\DSL\type_string;

#[RequiresPhp('>= 8.4.0')]
final class HtmlElementPhysicalTest extends TestCase
{
    public function test_the_markup(): void
    {
        $physical = new HtmlElementPhysical();
        $markup = type_string()->assert($physical->toPhysical(type_html_element()->cast('<p>a</p>')));

        static::assertSame('<p>a</p>', $markup);
        static::assertSame('<p>a</p>', $physical->toPhysical($physical->fromPhysical('<p>a</p>')));
        static::assertNull($physical->fromPhysicalAll(['<p>a</p>', null])[1]);
    }
}
