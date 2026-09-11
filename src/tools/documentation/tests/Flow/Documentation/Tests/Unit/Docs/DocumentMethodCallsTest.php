<?php

declare(strict_types=1);

namespace Flow\Documentation\Tests\Unit\Docs;

use Flow\Documentation\Docs\DocumentMethodCalls;
use Flow\Documentation\Tests\Context\FenceContext;
use PHPUnit\Framework\TestCase;

final class DocumentMethodCallsTest extends TestCase
{
    private FenceContext $fences;

    protected function setUp(): void
    {
        $this->fences = new FenceContext();
    }

    public function test_it_reports_a_method_the_receiver_does_not_declare(): void
    {
        $unresolved = (new DocumentMethodCalls())->unresolved([
            $this->fences->php("<?php\n\$s = new \\Flow\\ETL\\Schema(); \$s->thereIsNoSuchMethod();"),
        ]);

        static::assertSame(['a.md:1' => ['Flow\ETL\Schema::thereIsNoSuchMethod()']], $unresolved);
    }

    public function test_it_accepts_a_method_the_receiver_declares(): void
    {
        static::assertSame(
            [],
            (new DocumentMethodCalls())->unresolved([
                $this->fences->php("<?php\n\$s = new \\Flow\\ETL\\Schema(); \$s->definitions();"),
            ]),
        );
    }

    public function test_a_receiver_it_cannot_type_produces_no_verdict(): void
    {
        static::assertSame(
            [],
            (new DocumentMethodCalls())->unresolved([
                $this->fences->php("<?php\n\$unknown->thereIsNoSuchMethod();"),
            ]),
        );
    }

    public function test_a_self_referential_assignment_judges_the_call_before_the_reassignment(): void
    {
        static::assertSame(
            [],
            (new DocumentMethodCalls())->unresolved([
                $this->fences->php("<?php\n\$s = new \\Flow\\ETL\\Schema(); \$s = \$s->definitions();"),
            ]),
        );
    }

    public function test_a_receiver_typed_by_an_earlier_fence_is_not_judged(): void
    {
        static::assertSame(
            [],
            (new DocumentMethodCalls())->unresolved([
                $this->fences->php("<?php\n\$s = new \\Flow\\ETL\\Schema();"),
                $this->fences->php("<?php\n\$s->thereIsNoSuchMethod();"),
            ]),
        );
    }

    public function test_an_unparseable_fence_reports_nothing(): void
    {
        static::assertSame(
            [],
            (new DocumentMethodCalls())->unresolved([
                $this->fences->php('$s = new \\Flow\\ETL\\Schema(); $s->definitions()'),
            ]),
        );
    }
}
