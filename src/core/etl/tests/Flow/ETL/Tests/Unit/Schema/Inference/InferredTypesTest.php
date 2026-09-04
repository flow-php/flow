<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Schema\Inference;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Schema\Inference\InferredTypes;
use Flow\ETL\Tests\Fixtures\Enum\BackedStringEnum;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Types\Type;
use PHPUnit\Framework\Attributes\DataProvider;

use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_date;
use function Flow\Types\DSL\type_enum;
use function Flow\Types\DSL\type_html;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_mixed;
use function Flow\Types\DSL\type_null;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;
use function Flow\Types\DSL\type_time_zone;
use function Flow\Types\DSL\type_union;
use function Flow\Types\DSL\type_xml;

final class InferredTypesTest extends FlowTestCase
{
    /**
     * @return array<string, array{Type<mixed>}>
     */
    public static function rejectedTypes(): array
    {
        return [
            'list' => [type_list(type_string())],
            'map' => [type_map(type_string(), type_string())],
            'structure' => [type_structure(['a' => type_string()])],
            'union' => [type_union(type_integer(), type_string())],
            'mixed' => [type_mixed()],
            'null' => [type_null()],
        ];
    }

    /**
     * @param Type<mixed> $type
     */
    #[DataProvider('rejectedTypes')]
    public function test_a_container_or_open_type_cannot_be_inferred(Type $type): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('cannot be inferred from a candidate list');

        new InferredTypes($type);
    }

    public function test_an_empty_list_is_rejected(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('At least one type is required');

        new InferredTypes();
    }

    public function test_a_parameterised_type_admits_its_whole_family(): void
    {
        static::assertTrue((new InferredTypes(type_enum(BackedStringEnum::class)))->allows(type_enum(BackedStringEnum::class)));
    }

    public function test_default_is_every_rung_but_markup(): void
    {
        $types = InferredTypes::default();

        static::assertTrue($types->allows(type_integer()));
        static::assertTrue($types->allows(type_date()));
        static::assertTrue($types->allows(type_time_zone()));
        static::assertFalse($types->allows(type_html()));
        static::assertFalse($types->allows(type_xml()));
    }

    public function test_order_and_duplicates_do_not_matter(): void
    {
        static::assertEquals(
            new InferredTypes(type_integer(), type_string()),
            new InferredTypes(type_string(), type_integer(), type_integer()),
        );
    }

    public function test_string_is_admitted_even_when_it_is_not_listed(): void
    {
        static::assertTrue((new InferredTypes(type_integer()))->allows(type_string()));
    }

    public function test_a_type_outside_the_list_is_not_allowed(): void
    {
        $types = new InferredTypes(type_integer(), type_string());

        static::assertTrue($types->allows(type_integer()));
        static::assertFalse($types->allows(type_boolean()));
        static::assertFalse($types->allows(type_date()));
    }

    public function test_to_array_carries_the_list_the_narrower_receives(): void
    {
        static::assertEqualsCanonicalizing(
            [type_integer(), type_string()],
            (new InferredTypes(type_integer()))->toArray(),
        );
    }
}
