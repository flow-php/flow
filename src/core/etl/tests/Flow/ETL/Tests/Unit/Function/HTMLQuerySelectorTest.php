<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{config, flow_context, ref, row};
use Dom\{Element, HTMLDocument};
use Flow\ETL\Exception\RequiredPHPVersionException;
use PHPUnit\Framework\Attributes\RequiresPhp;
use PHPUnit\Framework\TestCase;

final class HTMLQuerySelectorTest extends TestCase
{
    #[RequiresPhp('< 8.4')]
    public function test_getting_element_for_older_versions() : void
    {
        $this->expectException(RequiredPHPVersionException::class);

        ref('value')->htmlQuerySelector('body div p')->eval(row(flow_context(config())->entryFactory()->create('value', '')));
    }

    #[RequiresPhp('>= 8.4')]
    public function test_getting_elements_for_given_path() : void
    {
        /* @phpstan-ignore-next-line */
        $html = HTMLDocument::createFromString('<!DOCTYPE html><html><head></head><body><div><span>foobar</span></div></body></html>');

        $result = ref('value')->htmlQuerySelector('body div span')->eval(row(flow_context(config())->entryFactory()->create('value', $html)));

        /* @phpstan-ignore-next-line */
        self::assertInstanceOf(Element::class, $result);
    }

    #[RequiresPhp('>= 8.4')]
    public function test_getting_null_when_nothing_found() : void
    {
        /* @phpstan-ignore-next-line */
        $html = HTMLDocument::createFromString('<!DOCTYPE html><html><head></head><body><div><span>foobar</span></div></body></html>');

        $result = ref('value')->htmlQuerySelector('body div p')->eval(row(flow_context(config())->entryFactory()->create('value', $html)));

        self::assertNull($result);
    }

    #[RequiresPhp('>= 8.4')]
    public function test_invalid_value() : void
    {
        $result = ref('value')->htmlQuerySelector('body div span')->eval(row(flow_context(config())->entryFactory()->create('value', '')));

        self::assertNull($result);
    }
}
