<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Reference\Expression;

use function Flow\ETL\DSL\cast;
use function Flow\ETL\DSL\ref;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row;
use Flow\ETL\Row\Factory\NativeEntryFactory;
use PHPUnit\Framework\TestCase;

final class CastTest extends TestCase
{
    public static function cast_provider() : array
    {
        $xml = new \DOMDocument();
        $xml->loadXML($xmlString = '<root><foo baz="buz">bar</foo></root>');

        return [
            'invalid' => [null, 'int', null],
            'int' => ['1', 'int', 1],
            'integer' => ['1', 'integer', 1],
            'float' => ['1', 'float', 1.0],
            'double' => ['1', 'double', 1.0],
            'real' => ['1', 'real', 1.0],
            'string' => [1, 'string', '1'],
            'bool' => ['1', 'bool', true],
            'boolean' => ['1', 'boolean', true],
            'array' => ['1', 'array', ['1']],
            'object' => ['1', 'object', (object) '1'],
            'null' => ['1', 'null', null],
            'json' => [[1], 'json', '[1]'],
            'json_pretty' => [[1], 'json_pretty', "[\n    1\n]"],
            'xml_to_array' => [$xml, 'array', ['root' => ['foo' => ['@attributes' => ['baz' => 'buz'], '@value' => 'bar']]]],
            'string_to_xml' => [$xmlString, 'xml', $xml],
        ];
    }

    /**
     * @dataProvider cast_provider
     */
    public function test_cast(mixed $from, string $to, mixed $expected) : void
    {
        $resultRefCast = ref('value')->cast($to)->eval(Row::create((new NativeEntryFactory())->create('value', $from)));
        $resultCastRef = cast(ref('value'), $to)->eval(Row::create((new NativeEntryFactory())->create('value', $from)));

        if (\is_object($expected) || \is_object($from)) {
            $this->assertEquals($expected, $resultRefCast);
            $this->assertEquals($expected, $resultCastRef);
        } else {
            $this->assertSame($expected, $resultRefCast);
            $this->assertSame($expected, $resultCastRef);
        }
    }

    public function test_casting_integer_to_xml() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Cannot cast integer to XML');

        ref('value')->cast('xml')->eval(Row::create((new NativeEntryFactory())->create('value', 1)));
    }

    public function test_casting_non_xml_string_to_xml() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Invalid XML string given: foo');

        ref('value')->cast('xml')->eval(Row::create((new NativeEntryFactory())->create('value', 'foo')));
    }
}
