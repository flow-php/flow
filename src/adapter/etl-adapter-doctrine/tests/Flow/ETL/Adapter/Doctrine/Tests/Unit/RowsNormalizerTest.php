<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine\Tests\Unit;

use function Flow\ETL\DSL\{integer_entry, string_entry, xml_element_entry, xml_entry};
use function Flow\ETL\DSL\{row, rows};
use Flow\ETL\Adapter\Doctrine\RowsNormalizer;
use PHPUnit\Framework\TestCase;

final class RowsNormalizerTest extends TestCase
{
    private RowsNormalizer $normalizer;

    protected function setUp() : void
    {
        $this->normalizer = new RowsNormalizer();
    }

    public function test_normalize_empty_rows() : void
    {
        $rows = rows();
        $result = $this->normalizer->normalize($rows);

        self::assertSame([], $result);
    }

    public function test_normalize_preserves_entry_order() : void
    {
        $xmlContent = '<user><name>John</name></user>';
        $doc = new \DOMDocument();
        $doc->loadXML($xmlContent);

        $rows = rows(row(string_entry('first', 'value1'), xml_entry('xml_data', $doc), integer_entry('number', 42), string_entry('last', 'value2')));

        $result = $this->normalizer->normalize($rows);

        self::assertCount(1, $result);
        $keys = array_keys($result[0]);
        self::assertSame(['first', 'xml_data', 'number', 'last'], $keys);
    }

    public function test_normalize_rows_with_mixed_xml_entries() : void
    {
        $xmlContent = '<root><user><name>John Doe</name></user></root>';
        $doc = new \DOMDocument();
        $doc->loadXML($xmlContent);
        $element = $doc->getElementsByTagName('user')[0];

        $xmlContent2 = '<profile><bio>Software Engineer</bio></profile>';
        $doc2 = new \DOMDocument();
        $doc2->loadXML($xmlContent2);

        $rows = rows(row(integer_entry('id', 1), xml_entry('profile', $doc2), xml_element_entry('user_element', $element), string_entry('status', 'active')));

        $result = $this->normalizer->normalize($rows);

        self::assertCount(1, $result);
        self::assertSame(1, $result[0]['id']);
        self::assertSame('active', $result[0]['status']);

        // Both XML entries should be converted to strings
        self::assertIsString($result[0]['profile']);
        self::assertIsString($result[0]['user_element']);
        self::assertStringContainsString('<profile>', $result[0]['profile']);
        self::assertStringContainsString('<bio>Software Engineer</bio>', $result[0]['profile']);
        self::assertStringContainsString('<user>', $result[0]['user_element']);
        self::assertStringContainsString('<name>John Doe</name>', $result[0]['user_element']);
    }

    public function test_normalize_rows_with_null_xml_entry() : void
    {
        $rows = rows(row(integer_entry('id', 1), xml_entry('user_data', null), string_entry('status', 'active')));

        $result = $this->normalizer->normalize($rows);

        self::assertCount(1, $result);
        self::assertSame(1, $result[0]['id']);
        self::assertSame('active', $result[0]['status']);
        self::assertSame('', $result[0]['user_data']); // null XML entry should become empty string
    }

    public function test_normalize_rows_with_xml_element_entry() : void
    {
        $xmlContent = '<root><user><name>John Doe</name></user></root>';
        $doc = new \DOMDocument();
        $doc->loadXML($xmlContent);
        $element = $doc->getElementsByTagName('user')[0];

        $rows = rows(row(integer_entry('id', 1), xml_element_entry('user_element', $element), string_entry('status', 'active')));

        $result = $this->normalizer->normalize($rows);

        self::assertCount(1, $result);
        self::assertSame(1, $result[0]['id']);
        self::assertSame('active', $result[0]['status']);
        self::assertIsString($result[0]['user_element']);
        self::assertStringContainsString('<user>', $result[0]['user_element']);
        self::assertStringContainsString('<name>John Doe</name>', $result[0]['user_element']);
    }

    public function test_normalize_rows_with_xml_entry() : void
    {
        $xmlContent = '<user><name>John Doe</name><email>john@example.com</email></user>';
        $doc = new \DOMDocument();
        $doc->loadXML($xmlContent);

        $rows = rows(row(integer_entry('id', 1), xml_entry('user_data', $doc), string_entry('status', 'active')));

        $result = $this->normalizer->normalize($rows);

        self::assertCount(1, $result);
        self::assertSame(1, $result[0]['id']);
        self::assertSame('active', $result[0]['status']);
        self::assertIsString($result[0]['user_data']);
        self::assertStringContainsString('<user>', $result[0]['user_data']);
        self::assertStringContainsString('<name>John Doe</name>', $result[0]['user_data']);
        self::assertStringContainsString('<email>john@example.com</email>', $result[0]['user_data']);
    }

    public function test_normalize_rows_without_xml_entries() : void
    {
        $rows = rows(row(integer_entry('id', 1), string_entry('name', 'John Doe'), string_entry('email', 'john@example.com')), row(integer_entry('id', 2), string_entry('name', 'Jane Smith'), string_entry('email', 'jane@example.com')));

        $result = $this->normalizer->normalize($rows);

        self::assertSame([
            ['id' => 1, 'name' => 'John Doe', 'email' => 'john@example.com'],
            ['id' => 2, 'name' => 'Jane Smith', 'email' => 'jane@example.com'],
        ], $result);
    }
}
