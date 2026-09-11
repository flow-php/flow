<?php

declare(strict_types=1);

namespace Flow\Documentation;

use ReflectionClass;
use ReflectionMethod;

final class MethodCollector
{
    /**
     * @var array<string>
     */
    public array $methods = [];

    /**
     * @param class-string|trait-string $className
     */
    public function collect(string $className): void
    {
        $reflectionClass = new ReflectionClass($className);
        $methods = $reflectionClass->getMethods(ReflectionMethod::IS_PUBLIC);

        foreach ($methods as $method) {
            if ($method->getDeclaringClass()->getName() !== $className) {
                continue;
            }

            // Skip magic methods __construct and __destruct
            if ($method->getName() === '__construct' || $method->getName() === '__destruct') {
                continue;
            }

            $this->methods[] = $method->getName();
        }
    }
}
