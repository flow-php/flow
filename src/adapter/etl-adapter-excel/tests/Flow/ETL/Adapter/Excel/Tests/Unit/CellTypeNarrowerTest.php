<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Excel\Tests\Unit;

use DateTimeImmutable;
use Flow\ETL\Adapter\Excel\CellTypeNarrower;
use Flow\ETL\Schema\Inference\InferredTypes;
use Flow\ETL\Tests\FlowTestCase;
use PHPUnit\Framework\Attributes\TestWith;

use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_string;

final class CellTypeNarrowerTest extends FlowTestCase
{
    #[TestWith(['254d61c5-22c8-4407-83a2-76f1cab53af2', 'uuid'])]
    #[TestWith(['{"street":"Main St"}', 'json'])]
    #[TestWith(['[{"sku":"A"}]', 'json'])]
    #[TestWith(['America/New_York', 'timezone'])]
    public function test_text_narrows_to_types_a_cell_cannot_hold(string $value, string $expected): void
    {
        static::assertSame(
            $expected,
            (new CellTypeNarrower(InferredTypes::default()))
                ->narrow($value)
                ->toString(),
        );
    }

    #[TestWith(['TRUE'])]
    #[TestWith(['12.9'])]
    #[TestWith(['2025-01-01T12:00:00+00:00'])]
    #[TestWith(['hello'])]
    public function test_text_stays_text_where_a_cell_type_exists(string $value): void
    {
        static::assertSame(
            'string',
            (new CellTypeNarrower(InferredTypes::default()))
                ->narrow($value)
                ->toString(),
        );
    }

    public function test_a_typed_cell_keeps_the_type_the_reader_gave_it(): void
    {
        $narrower = new CellTypeNarrower(InferredTypes::default());

        static::assertSame('integer', $narrower->narrow(1)->toString());
        static::assertSame('float', $narrower->narrow(1.5)->toString());
        static::assertSame('boolean', $narrower->narrow(true)->toString());
        static::assertSame('date', $narrower->narrow(new DateTimeImmutable('2024-01-01 00:00:00'))->toString());
        static::assertSame('datetime', $narrower->narrow(new DateTimeImmutable('2024-01-01 10:30:00'))->toString());
    }

    public function test_a_candidate_list_without_the_text_types_leaves_text_alone(): void
    {
        $narrower = new CellTypeNarrower(new InferredTypes(type_integer(), type_string()));

        static::assertSame('string', $narrower->narrow('254d61c5-22c8-4407-83a2-76f1cab53af2')->toString());
        static::assertSame('string', $narrower->narrow('{"a":1}')->toString());
        static::assertSame('integer', $narrower->narrow(1)->toString());
    }
}
