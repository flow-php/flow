<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\IO\Inference;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\IO\Inference\SchemaInference;
use Flow\ETL\Tests\Double\RecordingLauncher;
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
        return [[0], [-1], [-100]];
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
    public function test_files_to_sniff_rejects_anything_below_one(int $filesToSniff): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Use everySource() to infer from every source.');

        // @mago-ignore analysis:possibly-invalid-argument
        infer_schema()->filesToSniff($filesToSniff);
    }

    public function test_every_row_and_every_source_lift_both_bounds(): void
    {
        static::assertEquals(new SchemaInference(null, null), infer_schema()->everyRow()->everySource()->build());
    }

    public function test_every_row_keeps_a_finite_files_to_sniff(): void
    {
        static::assertEquals(new SchemaInference(null, 2), infer_schema()->filesToSniff(2)->everyRow()->build());
    }

    #[DataProvider('invalidBounds')]
    public function test_sample_size_rejects_anything_below_one(int $sampleSize): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Use everyRow() to infer from every row.');

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

    public function test_sniffs_in_process_by_default_and_through_what_sniff_with_names(): void
    {
        $launcher = new RecordingLauncher();

        static::assertNull(infer_schema()->build()->sniffs);
        static::assertSame($launcher, infer_schema()->sniffWith($launcher)->build()->sniffs);
    }
}
