<?php

declare(strict_types=1);

namespace Flow\Documentation\Tests\Context;

use Flow\Documentation\ClassesExtractor;
use Flow\Documentation\Models\ClassModel;
use Flow\Documentation\Models\MethodModel;

use function array_map;
use function dirname;
use function ksort;

final class ExtractedClassesContext
{
    /**
     * @param list<string> $roots
     *
     * @return array<string, ClassModel>
     */
    public function extract(array $roots): array
    {
        $classes = [];

        foreach ((new ClassesExtractor(dirname(__DIR__) . '/'))->extract($roots) as $class) {
            $classes[$class->name] = $class;
        }

        ksort($classes);

        return $classes;
    }

    /**
     * @return list<string>
     */
    public function methodNames(ClassModel $class): array
    {
        return array_map(static fn(MethodModel $method): string => $method->name, $class->methods);
    }

    /**
     * @return list<string>
     */
    public function staticMethodNames(ClassModel $class): array
    {
        return array_map(static fn(MethodModel $method): string => $method->name, $class->staticMethods);
    }
}
