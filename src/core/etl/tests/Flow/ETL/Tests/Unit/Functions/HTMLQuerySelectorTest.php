<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Functions;

use Dom\Element;
use Dom\HTMLDocument;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Foundation\Exception\RequiredPHPVersionException;
use Flow\ETL\Functions\HTMLQuerySelector;
use Flow\ETL\Tests\Context\DescribeRoundTrip;
use Flow\ETL\Tests\Context\FunctionContext;
use Flow\ETL\Tests\Context\ScalarFunctionFixtures;
use Flow\ETL\Values\Column\PhpBackend;
use PHPUnit\Framework\Attributes\RequiresPhp;
use PHPUnit\Framework\TestCase;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\html_schema;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class HTMLQuerySelectorTest extends TestCase
{
    #[RequiresPhp('< 8.4.0')]
    public function test_getting_element_for_older_versions(): void
    {
        $this->expectException(RequiredPHPVersionException::class);
        (new FunctionContext(flow_context()))->eval(
            ref('value')->htmlQuerySelector('body div p'),
            ['value' => ''],
            schema(str_schema('value')),
        );
    }

    #[RequiresPhp('>= 8.4.0')]
    public function test_getting_elements_for_given_path(): void
    {
        // @mago-ignore analysis:unavailable-method
        $html = HTMLDocument::createFromString(
            '<!DOCTYPE html><html><head></head><body><div><span>foobar</span></div></body></html>',
        );
        static::assertInstanceOf(Element::class, (new FunctionContext(flow_context()))->eval(
            ref('value')->htmlQuerySelector('body div span'),
            ['value' => $html],
            schema(html_schema('value')),
        ));
    }

    #[RequiresPhp('>= 8.4.0')]
    public function test_getting_null_when_nothing_found(): void
    {
        // @mago-ignore analysis:unavailable-method
        $html = HTMLDocument::createFromString(
            '<!DOCTYPE html><html><head></head><body><div><span>foobar</span></div></body></html>',
        );
        static::assertNull((new FunctionContext(flow_context()))->eval(
            ref('value')->htmlQuerySelector('body div p'),
            ['value' => $html],
            schema(html_schema('value')),
        ));
    }

    #[RequiresPhp('>= 8.4.0')]
    public function test_invalid_value(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expected one of');

        (new FunctionContext(flow_context()))->eval(
            ref('value')->htmlQuerySelector('body div span'),
            ['value' => ''],
            schema(str_schema('value')),
        );
    }

    public function test_describe_round_trips(): void
    {
        $context = new FunctionContext();

        try {
            $function = new HTMLQuerySelector(ref('html'), lit('div'));
        } catch (RequiredPHPVersionException $e) {
            static::markTestSkipped($e->getMessage());
        }

        $rebuilt = DescribeRoundTrip::rebuilt($function);
        $rows = ScalarFunctionFixtures::rows(new PhpBackend());
        $schema = ScalarFunctionFixtures::schema();

        static::assertSame($context->exported($function, $rows, $schema), $context->exported($rebuilt, $rows, $schema));
    }
}
