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

final class HTMLQuerySelectorAllTest extends TestCase
{
    #[RequiresPhp('< 8.4')]
    public function test_getting_element_for_older_versions(): void
    {
        $this->expectException(RequiredPHPVersionException::class);
        ref('value')
            ->htmlQuerySelectorAll('body div p')
            ->eval(row(flow_context(config())->entryFactory()->create('value', '')), flow_context());
    }

    #[RequiresPhp('>= 8.4')]
    public function test_getting_elements_for_given_path(): void
    {
        // @mago-ignore analysis:unavailable-method
        $html = HTMLDocument::createFromString(
            '<!DOCTYPE html><html><head></head><body><div><span>foobar</span></div></body></html>',
        );
        /** @var array<mixed> $result */
        $result = ref('value')
            ->htmlQuerySelectorAll('body div span')
            ->eval(row(flow_context(config())->entryFactory()->create('value', $html)), flow_context());
        static::assertCount(1, $result);
        static::assertInstanceOf(Element::class, $result[0]);
    }

    #[RequiresPhp('>= 8.4')]
    public function test_getting_null_when_nothing_found(): void
    {
        // @mago-ignore analysis:unavailable-method
        $html = HTMLDocument::createFromString(
            '<!DOCTYPE html><html><head></head><body><div><span>foobar</span></div></body></html>',
        );
        $result = ref('value')
            ->htmlQuerySelectorAll('body div p')
            ->eval(row(flow_context(config())->entryFactory()->create('value', $html)), flow_context());
        static::assertNull($result);
    }

    #[RequiresPhp('>= 8.4')]
    public function test_invalid_value(): void
    {
        $result = ref('value')
            ->htmlQuerySelectorAll('body div span')
            ->eval(row(flow_context(config())->entryFactory()->create('value', '')), flow_context());
        static::assertNull($result);
    }
}
