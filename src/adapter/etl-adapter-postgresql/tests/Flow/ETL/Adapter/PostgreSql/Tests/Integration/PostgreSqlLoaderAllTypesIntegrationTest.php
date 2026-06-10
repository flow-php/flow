<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql\Tests\Integration;

use DateInterval;
use DateTimeImmutable;
use DateTimeInterface;
use DOMDocument;
use Flow\ETL\Adapter\PostgreSql\Tests\Fixtures\Enum\BackedStringEnum;
use Flow\ETL\Adapter\PostgreSql\Tests\IntegrationTestCase;

use function Flow\ETL\Adapter\PostgreSql\from_pgsql_limit_offset;
use function Flow\ETL\Adapter\PostgreSql\to_pgsql_table;
use function Flow\ETL\DSL\bool_entry;
use function Flow\ETL\DSL\date_entry;
use function Flow\ETL\DSL\datetime_entry;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\enum_entry;
use function Flow\ETL\DSL\float_entry;
use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\json_entry;
use function Flow\ETL\DSL\list_entry;
use function Flow\ETL\DSL\map_entry;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\str_entry;
use function Flow\ETL\DSL\structure_entry;
use function Flow\ETL\DSL\time_entry;
use function Flow\ETL\DSL\uuid_entry;
use function Flow\ETL\DSL\xml_element_entry;
use function Flow\ETL\DSL\xml_entry;
use function Flow\PostgreSql\DSL\asc;
use function Flow\PostgreSql\DSL\col;
use function Flow\PostgreSql\DSL\column;
use function Flow\PostgreSql\DSL\column_type_bigint;
use function Flow\PostgreSql\DSL\column_type_boolean;
use function Flow\PostgreSql\DSL\column_type_custom;
use function Flow\PostgreSql\DSL\column_type_date;
use function Flow\PostgreSql\DSL\column_type_double_precision;
use function Flow\PostgreSql\DSL\column_type_jsonb;
use function Flow\PostgreSql\DSL\column_type_serial;
use function Flow\PostgreSql\DSL\column_type_text;
use function Flow\PostgreSql\DSL\column_type_time;
use function Flow\PostgreSql\DSL\column_type_timestamp;
use function Flow\PostgreSql\DSL\column_type_uuid;
use function Flow\PostgreSql\DSL\create;
use function Flow\PostgreSql\DSL\select;
use function Flow\PostgreSql\DSL\star;
use function Flow\PostgreSql\DSL\table;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;
use function is_string;
use function json_decode;
use function sprintf;

final class PostgreSqlLoaderAllTypesIntegrationTest extends IntegrationTestCase
{
    private string $tableName = 'flow_all_types_test';

    protected function setUp(): void
    {
        parent::setUp();

        $this->client->execute(
            create()
                ->table($this->tableName)
                ->column(column('id', column_type_serial())->primaryKey())
                ->column(column('col_string', column_type_text()))
                ->column(column('col_integer', column_type_bigint()))
                ->column(column('col_float', column_type_double_precision()))
                ->column(column('col_boolean', column_type_boolean()))
                ->column(column('col_date', column_type_date()))
                ->column(column('col_datetime', column_type_timestamp()))
                ->column(column('col_time', column_type_time()))
                ->column(column('col_uuid', column_type_uuid()))
                ->column(column('col_json', column_type_jsonb()))
                ->column(column('col_xml', column_type_custom('xml')))
                ->column(column('col_xml_element', column_type_custom('xml')))
                ->column(column('col_html', column_type_text()))
                ->column(column('col_html_element', column_type_text()))
                ->column(column('col_enum', column_type_text()))
                ->column(column('col_list', column_type_jsonb()))
                ->column(column('col_map', column_type_jsonb()))
                ->column(column('col_structure', column_type_jsonb())),
        );
    }

    public function test_inserts_all_entry_types(): void
    {
        $date = new DateTimeImmutable('2024-01-15');
        $dateTime = new DateTimeImmutable('2024-01-15 10:30:00+00:00');
        $time = new DateInterval('PT10H30M15S');
        $uuid = '550e8400-e29b-41d4-a716-446655440000';

        $xmlDoc = new DOMDocument();
        $xmlDoc->loadXML('<root><item>test</item></root>');

        $xmlElementDoc = new DOMDocument();
        $xmlElementDoc->loadXML('<root><item id="elem">element</item></root>');
        $xmlElement = $xmlElementDoc->getElementsByTagName('item')->item(0);

        df()
            ->read(from_rows(rows(row(
                str_entry('col_string', 'test string'),
                int_entry('col_integer', 42),
                float_entry('col_float', 3.14159),
                bool_entry('col_boolean', true),
                date_entry('col_date', $date),
                datetime_entry('col_datetime', $dateTime),
                time_entry('col_time', $time),
                uuid_entry('col_uuid', $uuid),
                json_entry('col_json', ['key' => 'value', 'number' => 123]),
                xml_entry('col_xml', $xmlDoc),
                xml_element_entry('col_xml_element', $xmlElement),
                str_entry('col_html', '<p>HTML content</p>'),
                str_entry('col_html_element', '<span>element</span>'),
                enum_entry('col_enum', BackedStringEnum::one),
                list_entry('col_list', [1, 2, 3], type_list(type_integer())),
                map_entry('col_map', ['a' => 1, 'b' => 2], type_map(type_string(), type_integer())),
                structure_entry('col_structure', ['name' => 'John', 'age' => 30], type_structure([
                    'name' => type_string(),
                    'age' => type_integer(),
                ])),
            ))))
            ->write(to_pgsql_table($this->client, $this->tableName))
            ->run();

        $result = df()
            ->read(from_pgsql_limit_offset(
                $this->client,
                select(star())->from(table($this->tableName))->orderBy(asc(col('id'))),
            ))
            ->fetch()
            ->toArray();

        static::assertCount(1, $result);
        $row = $result[0];

        static::assertSame('test string', $row['col_string']);
        static::assertSame(42, $row['col_integer']);
        static::assertEqualsWithDelta(3.14159, $row['col_float'], 0.00001);
        static::assertTrue($row['col_boolean']);
        static::assertSame(
            '2024-01-15',
            $row['col_date'] instanceof DateTimeInterface ? $row['col_date']->format('Y-m-d') : $row['col_date'],
        );
        // @mago-expect analysis:mixed-assignment
        $colDatetime = $row['col_datetime'];
        static::assertStringStartsWith(
            '2024-01-15',
            $colDatetime instanceof DateTimeInterface
                ? $colDatetime->format('Y-m-d H:i:s')
                : (is_scalar($colDatetime) ? (string) $colDatetime : ''),
        );
        static::assertSame(
            '10:30:15',
            $row['col_time'] instanceof DateInterval
                ? sprintf('%02d:%02d:%02d', $row['col_time']->h, $row['col_time']->i, $row['col_time']->s)
                : $row['col_time'],
        );
        static::assertSame($uuid, $row['col_uuid']);
        static::assertEquals(
            ['key' => 'value', 'number' => 123],
            is_string($row['col_json']) ? json_decode($row['col_json'], true) : $row['col_json'],
        );
        // @mago-expect analysis:mixed-assignment
        $colXml = $row['col_xml'];
        static::assertIsString($colXml);
        static::assertStringContainsString('<root><item>test</item></root>', $colXml);
        // @mago-expect analysis:mixed-assignment
        $colXmlElement = $row['col_xml_element'];
        static::assertIsString($colXmlElement);
        static::assertStringContainsString('<item id="elem">element</item>', $colXmlElement);
        static::assertSame('<p>HTML content</p>', $row['col_html']);
        static::assertSame('<span>element</span>', $row['col_html_element']);
        static::assertSame('one', $row['col_enum']);
        static::assertEquals(
            [1, 2, 3],
            is_string($row['col_list']) ? json_decode($row['col_list'], true) : $row['col_list'],
        );
        static::assertEquals(
            ['a' => 1, 'b' => 2],
            is_string($row['col_map']) ? json_decode($row['col_map'], true) : $row['col_map'],
        );
        static::assertEquals(
            ['name' => 'John', 'age' => 30],
            is_string($row['col_structure']) ? json_decode($row['col_structure'], true) : $row['col_structure'],
        );
    }

    public function test_inserts_null_values_for_all_entry_types(): void
    {
        df()
            ->read(from_rows(rows(row(
                str_entry('col_string', null),
                int_entry('col_integer', null),
                float_entry('col_float', null),
                bool_entry('col_boolean', null),
                date_entry('col_date', null),
                datetime_entry('col_datetime', null),
                time_entry('col_time', null),
                uuid_entry('col_uuid', null),
                json_entry('col_json', null),
                xml_entry('col_xml', null),
                xml_entry('col_xml_element', null),
                str_entry('col_html', null),
                str_entry('col_html_element', null),
                str_entry('col_enum', null),
                list_entry('col_list', null, type_list(type_integer())),
                map_entry('col_map', null, type_map(type_string(), type_integer())),
                structure_entry('col_structure', null, type_structure([
                    'name' => type_string(),
                    'age' => type_integer(),
                ])),
            ))))
            ->write(to_pgsql_table($this->client, $this->tableName))
            ->run();

        $result = df()
            ->read(from_pgsql_limit_offset(
                $this->client,
                select(star())->from(table($this->tableName))->orderBy(asc(col('id'))),
            ))
            ->fetch()
            ->toArray();

        static::assertCount(1, $result);
        $row = $result[0];

        static::assertNull($row['col_string']);
        static::assertNull($row['col_integer']);
        static::assertNull($row['col_float']);
        static::assertNull($row['col_boolean']);
        static::assertNull($row['col_date']);
        static::assertNull($row['col_datetime']);
        static::assertNull($row['col_time']);
        static::assertNull($row['col_uuid']);
        static::assertNull($row['col_json']);
        static::assertNull($row['col_xml']);
        static::assertNull($row['col_xml_element']);
        static::assertNull($row['col_html']);
        static::assertNull($row['col_html_element']);
        static::assertNull($row['col_enum']);
        static::assertNull($row['col_list']);
        static::assertNull($row['col_map']);
        static::assertNull($row['col_structure']);
    }
}
