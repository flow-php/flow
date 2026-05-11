<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Function;

interface CreateFunctionOptionsStep extends CreateFunctionFinalStep
{
    public function as(string $definition): CreateFunctionFinalStep;

    public function calledOnNullInput(): self;

    public function cost(int $cost): self;

    public function immutable(): self;

    public function language(string $language): self;

    public function leakproof(bool $value = true): self;

    public function parallel(ParallelSafety $safety): self;

    public function rows(int $rows): self;

    public function securityDefiner(): self;

    public function securityInvoker(): self;

    public function set(string $parameter, string $value): self;

    public function stable(): self;

    public function strict(): self;

    public function volatile(): self;
}
