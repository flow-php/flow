<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Schema\Inference;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Schema\Inference\SchemaInference;
use Flow\ETL\Tests\FlowTestCase;
use PHPUnit\Framework\Attributes\DataProvider;

use function Flow\ETL\DSL\infer_schema;
use function Flow\Types\DSL\type_date;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_string;

final class SchemaInferenceBuilderTest extends FlowTestCase
{
    /**
     * @return list<array{int}>
     */
    public static function invalidBounds(): array
    {
        return [[0], [-2], [-100]];
    }

    public function test_all_strings_is_sugar_for_a_string_only_candidate_set(): void
    {
        $candidates = infer_schema()->allStrings()->build()->candidates();

        static::assertTrue($candidates->allows(type_string()));
        static::assertFalse($candidates->allows(type_integer()));
        static::assertFalse($candidates->allows(type_date()));
    }

    public function test_an_empty_candidate_set_is_rejected(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('At least one type is required');

        infer_schema()->types();
    }

    public function test_a_container_type_is_not_a_candidate(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('cannot be inferred from a candidate list');

        infer_schema()->types(type_list(type_string()));
    }

    public function test_string_is_admitted_even_when_it_is_not_listed(): void
    {
        static::assertTrue(infer_schema()->types(type_integer())->build()->candidates()->allows(type_string()));
    }

    public function test_candidate_order_does_not_matter(): void
    {
        static::assertEquals(
            infer_schema()->types(type_integer(), type_string())->build()->candidates(),
            infer_schema()->types(type_string(), type_integer())->build()->candidates(),
        );
    }

    public function test_union_by_name_is_off_by_default_and_can_be_turned_on(): void
    {
        static::assertFalse(infer_schema()->build()->unionByName);
        static::assertTrue(infer_schema()->unionByName()->build()->unionByName);
        static::assertFalse(infer_schema()->unionByName()->unionByName(false)->build()->unionByName);
    }

    public function test_defaults_are_the_value_objects_defaults(): void
    {
        static::assertEquals(new SchemaInference(), infer_schema()->build());
    }

    #[DataProvider('invalidBounds')]
    public function test_files_to_sniff_rejects_anything_below_one_except_minus_one(int $filesToSniff): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Files to sniff must be greater than 0, or -1 for all sources');

        // @mago-ignore analysis:possibly-invalid-argument
        infer_schema()->filesToSniff($filesToSniff);
    }

    public function test_minus_one_is_accepted_for_both_bounds(): void
    {
        static::assertEquals(new SchemaInference(-1, -1), infer_schema()->sampleSize(-1)->filesToSniff(-1)->build());
    }

    #[DataProvider('invalidBounds')]
    public function test_sample_size_rejects_anything_below_one_except_minus_one(int $sampleSize): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Sample size must be greater than 0, or -1 for all rows');

        // @mago-ignore analysis:possibly-invalid-argument
        infer_schema()->sampleSize($sampleSize);
    }

    public function test_every_setter_is_carried_into_build(): void
    {
        $inference = infer_schema()->sampleSize(100)->filesToSniff(3)->allStrings()->unionByName()->build();

        static::assertSame(100, $inference->sampleSize);
        static::assertSame(3, $inference->filesToSniff);
        static::assertTrue($inference->unionByName);
        static::assertFalse($inference->candidates()->allows(type_integer()));
    }
}
