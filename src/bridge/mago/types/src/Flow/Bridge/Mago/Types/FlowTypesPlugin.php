<?php

declare(strict_types=1);

namespace Flow\Bridge\Mago\Types;

use Mago\Sdk\Analyzer\Plugin;
use Mago\Sdk\Analyzer\PluginDefinition;
use Mago\Sdk\Analyzer\PluginRegistry;

final class FlowTypesPlugin implements Plugin
{
    public function getDefinition(): PluginDefinition
    {
        return new PluginDefinition(
            identifier: 'flow/types',
            name: 'Flow Types',
            description: 'Shape derivation for the flow-php/types DSL, including structure_element() markers.',
        );
    }

    public function register(PluginRegistry $registry): void
    {
        $registry->registerFunctionReturnTypeProvider(new TypeStructureReturnTypeProvider());
    }
}
