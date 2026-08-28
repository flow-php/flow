<?php

declare(strict_types=1);

namespace Flow\Documentation;

use Flow\Documentation\Models\MethodModel;
use Generator;
use ReflectionClass;

use function ltrim;
use function str_replace;

final readonly class MethodsExtractor
{
    /**
     * @param class-string|trait-string $className
     */
    public function __construct(
        private string $repositoryRootPath,
        private string $className,
        private MethodCollector $methodCollector,
    ) {}

    /**
     * @return \Generator<MethodModel>
     */
    public function extract(): Generator
    {
        $this->methodCollector->collect($this->className);

        $reflectionClass = new ReflectionClass($this->className);

        foreach ($this->methodCollector->methods as $methodName) {
            $reflectionMethod = $reflectionClass->getMethod($methodName);
            $repositoryPath = ltrim(
                str_replace($this->repositoryRootPath, '', (string) $reflectionMethod->getFileName()),
                '/',
            );

            yield MethodModel::fromReflection($repositoryPath, $reflectionMethod);
        }
    }
}
