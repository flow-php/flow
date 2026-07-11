<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Unit;

use Flow\Floe\Exception\FloeException;
use Flow\Floe\Footer;
use Flow\Floe\Section;
use Flow\Floe\Tests\Mother\FooterMother;
use PHPUnit\Framework\TestCase;

use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Types\DSL\type_integer;
use function json_decode;
use function json_encode;

use const JSON_THROW_ON_ERROR;

final class FooterTest extends TestCase
{
    public function test_empty_footer_encodes_maps_as_json_objects(): void
    {
        $json = FooterMother::footer()->toJson();

        static::assertStringContainsString('"partitions":{}', $json);
        static::assertStringContainsString('"metadata":{}', $json);
        static::assertStringContainsString('"schemas":[]', $json);
        static::assertStringContainsString('"sections":[]', $json);
    }

    public function test_file_schema_round_trips_through_schema_object(): void
    {
        $schema = schema(int_schema('id'), str_schema('name', nullable: true));
        $footer = FooterMother::footer(fileSchema: $schema->normalize());

        static::assertTrue($schema->isSame(Footer::fromJson($footer->toJson())->fileSchema()));
    }

    public function test_from_json_requires_all_fields(): void
    {
        $this->expectException(FloeException::class);

        Footer::fromJson(
            '{"version":1,"writer":"x","schemas":[],"fileSchema":[],"partitions":{},"totalRows":0,"metadata":{}}',
        );
    }

    public function test_from_json_with_malformed_json_throws(): void
    {
        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('failed to decode footer JSON');

        Footer::fromJson('{broken');
    }

    public function test_from_json_with_non_integer_version_throws(): void
    {
        $this->expectException(FloeException::class);

        Footer::fromJson('{"version":"1"}');
    }

    public function test_from_json_with_non_integer_total_rows_throws(): void
    {
        $this->expectException(FloeException::class);

        Footer::fromJson('{"version":1,"totalRows":"0"}');
    }

    public function test_from_json_with_non_object_footer_throws(): void
    {
        $this->expectException(FloeException::class);

        Footer::fromJson('"scalar"');
    }

    public function test_from_json_with_non_object_section_throws(): void
    {
        /** @var array<string, mixed> $footer */
        $footer = json_decode(FooterMother::footer()->toJson(), true, 512, JSON_THROW_ON_ERROR);
        $footer['sections'] = [1];

        $this->expectException(FloeException::class);

        Footer::fromJson(json_encode($footer, JSON_THROW_ON_ERROR));
    }

    public function test_from_json_with_non_string_writer_throws(): void
    {
        $this->expectException(FloeException::class);

        Footer::fromJson('{"version":1,"totalRows":0,"writer":42}');
    }

    public function test_from_json_round_trips_typed_metadata(): void
    {
        $footer = FooterMother::footer(metadata: [
            'row_count' => 42,
            'ratio' => 1.5,
            'sorted' => true,
            'source' => 'bucket-7',
            'id_stats' => ['min' => 1, 'max' => 1000],
        ]);

        $decoded = Footer::fromJson($footer->toJson());

        static::assertEquals($footer, $decoded);
        static::assertSame(42, $decoded->metadata->getAs('row_count', type_integer()));
        static::assertSame(['min' => 1, 'max' => 1000], $decoded->metadata->get('id_stats'));
    }

    public function test_from_json_with_non_scalar_metadata_value_throws(): void
    {
        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('metadata is malformed');

        Footer::fromJson(
            '{"version":1,"writer":"x","schemas":[],"fileSchema":[],"sections":[],"partitions":{},"totalRows":0,"metadata":{"bad":null}}',
        );
    }

    public function test_schema_body_re_encodes_stored_schema(): void
    {
        $schema = schema(int_schema('id'));
        $footer = FooterMother::footer(schemas: [$schema->normalize()]);

        static::assertJsonStringEqualsJsonString(
            json_encode($schema->normalize(), JSON_THROW_ON_ERROR),
            Footer::fromJson($footer->toJson())->schemaBody(0),
        );
    }

    public function test_schema_body_with_unknown_id_throws(): void
    {
        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('does not hold schema with id 3');

        FooterMother::footer()->schemaBody(3);
    }

    public function test_to_json_round_trips_every_field(): void
    {
        $footer = FooterMother::footer(
            schemas: [schema(int_schema('id'))->normalize()],
            fileSchema: schema(int_schema('id'))->normalize(),
            sections: [new Section(6, 0, 2), new Section(120, 0, 3)],
            partitions: ['country' => 'PL'],
            totalRows: 5,
            metadata: ['source' => 'test'],
        );

        static::assertEquals($footer, Footer::fromJson($footer->toJson()));
    }
}
