<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Extension;

interface CreateExtensionOptionsStep extends CreateExtensionFinalStep
{
    public function cascade(): self;

    public function ifNotExists(): self;

    public function schema(string $schema): self;

    public function version(string $version): self;
}
