<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql\Tests\Integration;

use function Flow\ETL\Adapter\PostgreSql\{from_pgsql_limit_offset, to_pgsql_table};
use function Flow\ETL\DSL\{bool_entry, date_entry, datetime_entry, df, enum_entry, float_entry, from_rows, int_entry, json_entry, list_entry, map_entry, row, rows, str_entry, structure_entry, time_entry, uuid_entry, xml_element_entry, xml_entry};
use function Flow\PostgreSql\DSL\{column, create, data_type_bigint, data_type_boolean, data_type_custom, data_type_date, data_type_double_precision, data_type_jsonb, data_type_serial, data_type_text, data_type_time, data_type_timestamptz, data_type_uuid, drop, select, star, table};
use function Flow\Types\DSL\{type_integer, type_list, type_map, type_string, type_structure};
use Flow\ETL\Adapter\PostgreSql\Tests\Fixtures\Enum\BackedStringEnum;
use Flow\ETL\Adapter\PostgreSql\Tests\IntegrationTestCase;

final class PostgreSqlLoaderAllTypesIntegrationTest extends IntegrationTestCase
{
    private string $tableName = 'flow_all_types_test';

    protected function setUp() : void
    {
        parent::setUp();

        $this->client->execute(
            drop()->table($this->tableName)->ifExists()->cascade()
        );

        $this->client->execute(
            create()->table($this->tableName)
                ->column(column('id', data_type_serial())->primaryKey())
                ->column(column('col_string', data_type_text()))
                ->column(column('col_integer', data_type_bigint()))
                ->column(column('col_float', data_type_double_precision()))
                ->column(column('col_boolean', data_type_boolean()))
                ->column(column('col_date', data_type_date()))
                ->column(column('col_datetime', data_type_timestamptz()))
                ->column(column('col_time', data_type_time()))
                ->column(column('col_uuid', data_type_uuid()))
                ->column(column('col_json', data_type_jsonb()))
                ->column(column('col_xml', data_type_custom('xml')))
                ->column(column('col_xml_element', data_type_custom('xml')))
                ->column(column('col_html', data_type_text()))
                ->column(column('col_html_element', data_type_text()))
                ->column(column('col_enum', data_type_text()))
                ->column(column('col_list', data_type_jsonb()))
                ->column(column('col_map', data_type_jsonb()))
                ->column(column('col_structure', data_type_jsonb()))
        );
    }

    protected function tearDown() : void
    {
        if (isset($this->client)) {
            $this->client->execute(
                drop()->table($this->tableName)->ifExists()->cascade()
            );
        }

        parent::tearDown();
    }

    public function test_inserts_all_entry_types() : void
    {
        $date = new \DateTimeImmutable('2024-01-15');
        $dateTime = new \DateTimeImmutable('2024-01-15 10:30:00+00:00');
        $time = new \DateInterval('PT10H30M15S');
        $uuid = '550e8400-e29b-41d4-a716-446655440000';

        $xmlDoc = new \DOMDocument();
        $xmlDoc->loadXML('<root><item>test</item></root>');

        $xmlElementDoc = new \DOMDocument();
        $xmlElementDoc->loadXML('<root><item id="elem">element</item></root>');
        $xmlElement = $xmlElementDoc->getElementsByTagName('item')->item(0);

        df()
            ->read(from_rows(rows(
                row(
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
                )
            )))
            ->write(to_pgsql_table($this->client, $this->tableName))
            ->run();

        $result = df()
            ->read(from_pgsql_limit_offset(
                $this->client,
                select(star())->from(table($this->tableName)),
            ))
            ->fetch()
            ->toArray();

        self::assertCount(1, $result);
        $row = $result[0];

        self::assertSame('test string', $row['col_string']);
        self::assertSame(42, $row['col_integer']);
        self::assertEqualsWithDelta(3.14159, $row['col_float'], 0.00001);
        self::assertTrue($row['col_boolean']);
        self::assertSame('2024-01-15', $row['col_date'] instanceof \DateTimeInterface ? $row['col_date']->format('Y-m-d') : $row['col_date']);
        self::assertStringStartsWith('2024-01-15', $row['col_datetime'] instanceof \DateTimeInterface ? $row['col_datetime']->format('Y-m-d H:i:s') : $row['col_datetime']);
        self::assertSame('10:30:15', $row['col_time'] instanceof \DateInterval ? \sprintf('%02d:%02d:%02d', $row['col_time']->h, $row['col_time']->i, $row['col_time']->s) : $row['col_time']);
        self::assertSame($uuid, $row['col_uuid']);
        self::assertEquals(['key' => 'value', 'number' => 123], \is_string($row['col_json']) ? \json_decode($row['col_json'], true) : $row['col_json']);
        self::assertStringContainsString('<root><item>test</item></root>', $row['col_xml']);
        self::assertStringContainsString('<item id="elem">element</item>', $row['col_xml_element']);
        self::assertSame('<p>HTML content</p>', $row['col_html']);
        self::assertSame('<span>element</span>', $row['col_html_element']);
        self::assertSame('one', $row['col_enum']);
        self::assertEquals([1, 2, 3], \is_string($row['col_list']) ? \json_decode($row['col_list'], true) : $row['col_list']);
        self::assertEquals(['a' => 1, 'b' => 2], \is_string($row['col_map']) ? \json_decode($row['col_map'], true) : $row['col_map']);
        self::assertEquals(['name' => 'John', 'age' => 30], \is_string($row['col_structure']) ? \json_decode($row['col_structure'], true) : $row['col_structure']);
    }

    public function test_inserts_null_values_for_all_entry_types() : void
    {
        df()
            ->read(from_rows(rows(
                row(
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
                )
            )))
            ->write(to_pgsql_table($this->client, $this->tableName))
            ->run();

        $result = df()
            ->read(from_pgsql_limit_offset(
                $this->client,
                select(star())->from(table($this->tableName)),
            ))
            ->fetch()
            ->toArray();

        self::assertCount(1, $result);
        $row = $result[0];

        self::assertNull($row['col_string']);
        self::assertNull($row['col_integer']);
        self::assertNull($row['col_float']);
        self::assertNull($row['col_boolean']);
        self::assertNull($row['col_date']);
        self::assertNull($row['col_datetime']);
        self::assertNull($row['col_time']);
        self::assertNull($row['col_uuid']);
        self::assertNull($row['col_json']);
        self::assertNull($row['col_xml']);
        self::assertNull($row['col_xml_element']);
        self::assertNull($row['col_html']);
        self::assertNull($row['col_html_element']);
        self::assertNull($row['col_enum']);
        self::assertNull($row['col_list']);
        self::assertNull($row['col_map']);
        self::assertNull($row['col_structure']);
    }
}
