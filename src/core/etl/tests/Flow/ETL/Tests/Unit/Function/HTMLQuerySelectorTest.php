<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Dom\Element;
use Dom\HTMLDocument;
use Flow\ETL\Exception\RequiredPHPVersionException;
use PHPUnit\Framework\Attributes\RequiresPhp;
use PHPUnit\Framework\TestCase;

use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;

final class HTMLQuerySelectorTest extends TestCase
{
    #[RequiresPhp('< 8.4')]
    public function test_getting_element_for_older_versions(): void
    {
        $this->expectException(RequiredPHPVersionException::class);
        ref('value')
            ->htmlQuerySelector('body div p')
            ->eval(row(flow_context(config())->entryFactory()->create('value', '')), flow_context());
    }

    #[RequiresPhp('>= 8.4')]
    public function test_getting_elements_for_given_path(): void
    {
        // @mago-ignore analysis:unavailable-method
        $html = HTMLDocument::createFromString(
            '<!DOCTYPE html><html><head></head><body><div><span>foobar</span></div></body></html>',
        );
        $result = ref('value')
            ->htmlQuerySelector('body div span')
            ->eval(row(flow_context(config())->entryFactory()->create('value', $html)), flow_context());
        static::assertInstanceOf(Element::class, $result);
    }

    #[RequiresPhp('>= 8.4')]
    public function test_getting_null_when_nothing_found(): void
    {
        // @mago-ignore analysis:unavailable-method
        $html = HTMLDocument::createFromString(
            '<!DOCTYPE html><html><head></head><body><div><span>foobar</span></div></body></html>',
        );
        $result = ref('value')
            ->htmlQuerySelector('body div p')
            ->eval(row(flow_context(config())->entryFactory()->create('value', $html)), flow_context());
        static::assertNull($result);
    }

    #[RequiresPhp('>= 8.4')]
    public function test_invalid_value(): void
    {
        $result = ref('value')
            ->htmlQuerySelector('body div span')
            ->eval(row(flow_context(config())->entryFactory()->create('value', '')), flow_context());
        static::assertNull($result);
    }
}
