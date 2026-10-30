<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\IO\Inference;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\IO\Inference\InferredTypes;
use Flow\ETL\IO\Inference\SchemaInference;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Types\Exception\InvalidTypeException;
use PHPUnit\Framework\Attributes\TestWith;

use function Flow\Types\DSL\type_date;
use function Flow\Types\DSL\type_html;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_time_zone;
use function Flow\Types\DSL\type_xml;

final class SchemaInferenceTest extends FlowTestCase
{
    public function test_defaults(): void
    {
        $inference = new SchemaInference();

        static::assertSame(20_480, $inference->sampleSize);
        static::assertSame(10, $inference->filesToSniff);
        static::assertNull($inference->types);
        static::assertFalse($inference->unionByName);
    }

    public function test_the_default_candidate_set_is_every_rung_but_markup(): void
    {
        $candidates = (new SchemaInference())->candidates();

        static::assertTrue($candidates->allows(type_integer()));
        static::assertTrue($candidates->allows(type_date()));
        static::assertTrue($candidates->allows(type_time_zone()));
        static::assertFalse($candidates->allows(type_html()));
        static::assertFalse($candidates->allows(type_xml()));
    }

    public function test_normalize_round_trips_with_and_without_candidates(): void
    {
        $default = new SchemaInference();
        $custom = new SchemaInference(-1, -1, new InferredTypes(type_integer(), type_date()), true);

        static::assertSame(
            ['sample_size' => 20_480, 'files_to_sniff' => 10, 'types' => null, 'union_by_name' => false],
            $default->normalize(),
        );
        static::assertEquals($default, SchemaInference::fromArray($default->normalize()));
        static::assertSame(
            [type_integer()->normalize(), type_date()->normalize(), type_string()->normalize()],
            $custom->normalize()['types'],
        );
        static::assertEquals($custom, SchemaInference::fromArray($custom->normalize()));
    }

    #[TestWith([0, 10, 'Sample size must be greater than 0, or -1 for all rows'])]
    #[TestWith([-2, 10, 'Sample size must be greater than 0, or -1 for all rows'])]
    #[TestWith([100, 0, 'Files to sniff must be greater than 0, or -1 for all sources'])]
    public function test_a_size_below_one_other_than_minus_one_is_refused(
        int $sampleSize,
        int $filesToSniff,
        string $message,
    ): void {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage($message);

        // @mago-expect analysis:possibly-invalid-argument(2)
        new SchemaInference($sampleSize, $filesToSniff);
    }

    public function test_from_array_refuses_what_the_constructor_refuses(): void
    {
        $this->expectException(InvalidTypeException::class);

        SchemaInference::fromArray([
            'sample_size' => 0,
            'files_to_sniff' => 1,
            'types' => null,
            'union_by_name' => false,
        ]);
    }
}
