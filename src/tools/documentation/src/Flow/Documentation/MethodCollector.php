<?php

declare(strict_types=1);

namespace Flow\Documentation;

final class MethodCollector
{
    /**
     * @var array<string>
     */
    public array $methods = [];

    /**
     * @param class-string $className
     */
    public function collect(string $className) : void
    {
        $reflectionClass = new \ReflectionClass($className);
        $methods = $reflectionClass->getMethods(\ReflectionMethod::IS_PUBLIC);

        foreach ($methods as $method) {
            if ($method->getDeclaringClass()->getName() !== $className) {
                continue;
            }

            $this->methods[] = $method->getName();
        }
    }
}
