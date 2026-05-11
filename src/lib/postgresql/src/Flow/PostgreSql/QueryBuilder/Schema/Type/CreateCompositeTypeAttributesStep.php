<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Type;

interface CreateCompositeTypeAttributesStep
{
    public function attributes(TypeAttribute ...$attributes): CreateCompositeTypeFinalStep;
}
