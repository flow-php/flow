<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Column\Physical;

use Flow\ETL\Column\Physical\HtmlDocumentPhysical;
use Flow\ETL\Exception\InvalidArgumentException;
use PHPUnit\Framework\Attributes\RequiresPhp;
use PHPUnit\Framework\TestCase;

use function Flow\Types\DSL\type_html;
use function Flow\Types\DSL\type_string;

final class HtmlDocumentPhysicalTest extends TestCase
{
    #[RequiresPhp('>= 8.4.0')]
    public function test_the_markup(): void
    {
        $document = type_html()->cast('<!DOCTYPE html><html><head></head><body><p>a</p></body></html>');
        $physical = new HtmlDocumentPhysical();
        $markup = type_string()->assert($physical->toPhysical($document));

        static::assertIsString($markup);
        static::assertStringContainsString('<p>a</p>', $markup);
        static::assertEquals($markup, $physical->toPhysical($physical->fromPhysical($markup)));
        static::assertNull($physical->fromPhysicalAll([$markup, null])[1]);
    }

    #[RequiresPhp('< 8.4.0')]
    public function test_restoring_below_php84_throws(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('requires PHP 8.4+');

        (new HtmlDocumentPhysical())->fromPhysical('<p>x</p>');
    }
}
