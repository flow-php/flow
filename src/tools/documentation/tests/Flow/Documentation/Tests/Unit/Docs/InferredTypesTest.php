<?php

declare(strict_types=1);

namespace Flow\Documentation\Tests\Unit\Docs;

use Flow\Documentation\Docs\DocumentMethodCalls;
use Flow\Documentation\Tests\Context\FenceContext;
use PHPUnit\Framework\TestCase;

/**
 * Asserted through DocumentMethodCalls, because a resolved type is only observable as a verdict.
 * Every case asserts a bogus call IS reported - a negative-only suite cannot tell a working
 * analyzer from a dead one.
 */
final class InferredTypesTest extends TestCase
{
    private FenceContext $fences;

    protected function setUp(): void
    {
        $this->fences = new FenceContext();
    }

    public function test_a_receiver_typed_from_a_dsl_function_call_is_judged(): void
    {
        static::assertSame(
            ['a.md:1' => ['Flow\ETL\Flow::thereIsNoSuchMethod()']],
            (new DocumentMethodCalls())->unresolved([
                $this->fences->php(
                    "<?php\n\nuse function Flow\\ETL\\DSL\\data_frame;\n\ndata_frame()->thereIsNoSuchMethod();",
                ),
            ]),
        );
    }

    public function test_a_receiver_typed_through_a_fluent_chain_is_judged(): void
    {
        static::assertSame(
            ['a.md:1' => ['Flow\ETL\DataFrame::thereIsNoSuchMethod()']],
            (new DocumentMethodCalls())->unresolved([
                $this->fences->php(
                    "<?php\n\nuse function Flow\\ETL\\DSL\\{data_frame, from_array};\n\n"
                    . 'data_frame()->read(from_array([]))->thereIsNoSuchMethod();',
                ),
            ]),
        );
    }

    public function test_a_receiver_typed_from_a_static_call_is_judged(): void
    {
        static::assertSame(
            ['a.md:1' => ['Flow\Parquet\ParquetFile\Schema::thereIsNoSuchMethod()']],
            (new DocumentMethodCalls())->unresolved([
                $this->fences->php("<?php\n\n\\Flow\\Parquet\\ParquetFile\\Schema::with()->thereIsNoSuchMethod();"),
            ]),
        );
    }

    public function test_a_nullsafe_call_is_judged_like_any_other(): void
    {
        static::assertSame(
            ['a.md:1' => ['Flow\ETL\Flow::thereIsNoSuchMethod()']],
            (new DocumentMethodCalls())->unresolved([
                $this->fences->php(
                    "<?php\n\nuse function Flow\\ETL\\DSL\\data_frame;\n\ndata_frame()?->thereIsNoSuchMethod();",
                ),
            ]),
        );
    }

    public function test_a_real_method_on_a_resolved_receiver_is_accepted(): void
    {
        static::assertSame(
            [],
            (new DocumentMethodCalls())->unresolved([
                $this->fences->php("<?php\n\nuse function Flow\\ETL\\DSL\\data_frame;\n\ndata_frame()->read(\$e);"),
            ]),
        );
    }
}
