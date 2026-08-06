<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\DependencyInjection\Compiler;

use Symfony\Component\DependencyInjection\ChildDefinition;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\DependencyInjection\Definition;
use Symfony\Component\DependencyInjection\Exception\ParameterNotFoundException;

use function array_key_exists;
use function is_string;

final readonly class DefinitionClassResolver
{
    public function __construct(
        private ContainerBuilder $container,
    ) {}

    /**
     * @return null|class-string
     */
    public function resolve(Definition $definition): ?string
    {
        $seen = [];

        while ($definition->getClass() === null && $definition instanceof ChildDefinition) {
            $parent = $definition->getParent();

            if (array_key_exists($parent, $seen) || !$this->container->has($parent)) {
                return null;
            }

            $seen[$parent] = true;
            $definition = $this->container->findDefinition($parent);
        }

        try {
            /** @var mixed $class */
            $class = $this->container->getParameterBag()->resolveValue($definition->getClass());
        } catch (ParameterNotFoundException) {
            return null;
        }

        if (!is_string($class)) {
            return null;
        }

        return $this->container->getReflectionClass($class, false)?->getName();
    }
}
