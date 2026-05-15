<?php

declare(strict_types=1);

namespace Flow\Azure\SDK\Tests\Unit\Normalizer;

use Flow\Azure\SDK\Normalizer\SimpleXMLNormalizer;
use PHPUnit\Framework\TestCase;

final class SimpleXMLNormalizerTest extends TestCase
{
    public function test_to_array_converts_flat_xml_to_associative_array(): void
    {
        $xml = <<<'XML'
            <?xml version="1.0" encoding="utf-8"?>
            <Root>
                <Name>example</Name>
                <Size>1234</Size>
            </Root>
            XML;

        static::assertSame(['Name' => 'example', 'Size' => '1234'], (new SimpleXMLNormalizer())->toArray($xml));
    }

    public function test_to_array_normalizes_nested_elements_to_nested_arrays(): void
    {
        $xml = <<<'XML'
            <?xml version="1.0" encoding="utf-8"?>
            <Root>
                <Properties>
                    <Last-Modified>Mon, 25 Dec 2023 12:00:00 GMT</Last-Modified>
                    <Content-Length>5678</Content-Length>
                </Properties>
            </Root>
            XML;

        static::assertSame(
            [
                'Properties' => [
                    'Last-Modified' => 'Mon, 25 Dec 2023 12:00:00 GMT',
                    'Content-Length' => '5678',
                ],
            ],
            (new SimpleXMLNormalizer())->toArray($xml),
        );
    }

    public function test_to_array_normalizes_empty_elements_to_null(): void
    {
        $xml = <<<'XML'
            <?xml version="1.0" encoding="utf-8"?>
            <Root>
                <Empty/>
            </Root>
            XML;

        static::assertSame(['Empty' => null], (new SimpleXMLNormalizer())->toArray($xml));
    }

    public function test_to_array_normalizes_repeated_siblings_into_indexed_array(): void
    {
        $xml = <<<'XML'
            <?xml version="1.0" encoding="utf-8"?>
            <Blocks>
                <Block>
                    <Name>a</Name>
                    <Size>1</Size>
                </Block>
                <Block>
                    <Name>b</Name>
                    <Size>2</Size>
                </Block>
            </Blocks>
            XML;

        static::assertSame(
            [
                'Block' => [
                    ['Name' => 'a', 'Size' => '1'],
                    ['Name' => 'b', 'Size' => '2'],
                ],
            ],
            (new SimpleXMLNormalizer())->toArray($xml),
        );
    }

    public function test_to_array_normalizes_single_repeated_element_as_associative_array(): void
    {
        $xml = <<<'XML'
            <?xml version="1.0" encoding="utf-8"?>
            <Blocks>
                <Block>
                    <Name>only</Name>
                    <Size>9</Size>
                </Block>
            </Blocks>
            XML;

        static::assertSame(['Block' => ['Name' => 'only', 'Size' => '9']], (new SimpleXMLNormalizer())->toArray($xml));
    }
}
