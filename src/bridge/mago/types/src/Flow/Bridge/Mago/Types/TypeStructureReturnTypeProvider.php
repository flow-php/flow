<?php

declare(strict_types=1);

namespace Flow\Bridge\Mago\Types;

use Mago\Sdk\Analyzer\FunctionReturnTypeProvider;
use Mago\Sdk\Analyzer\FunctionTarget;
use Mago\Sdk\Analyzer\ReturnTypeProviderContext;
use Mago\Sdk\Analyzer\Type;

final class TypeStructureReturnTypeProvider implements FunctionReturnTypeProvider
{
    public function getTargets(): array
    {
        return [FunctionTarget::exact('Flow\Types\DSL\type_structure')];
    }

    public function getReturnType(ReturnTypeProviderContext $context): ?Type
    {
        return (new TypeStructureShape())->derive($context->invocation);
    }
}
