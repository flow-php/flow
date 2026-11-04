<?php

declare(strict_types=1);

namespace Flow\Documentation\Tests\Context;

use Flow\Documentation\ClassesExtractor;
use Flow\Documentation\Models\ClassModel;
use Flow\Documentation\Models\MethodModel;
use Roave\BetterReflection\BetterReflection;
use Roave\BetterReflection\Reflector\DefaultReflector;
use Roave\BetterReflection\SourceLocator\SourceStubber\PhpStormStubsSourceStubber;
use Roave\BetterReflection\SourceLocator\Type\AggregateSourceLocator;
use Roave\BetterReflection\SourceLocator\Type\AutoloadSourceLocator;
use Roave\BetterReflection\SourceLocator\Type\PhpInternalSourceLocator;

use function array_map;
use function dirname;
use function ksort;

final class ExtractedClassesContext
{
    /**
     * @param list<string> $roots
     * @param list<string> $phpExtensions
     *
     * @return array<string, ClassModel>
     */
    public function extract(array $roots, array $phpExtensions = []): array
    {
        $betterReflection = new BetterReflection();
        $classes = [];

        foreach ((new ClassesExtractor(
            dirname(__DIR__) . '/',
            new DefaultReflector(new AggregateSourceLocator([
                new AutoloadSourceLocator($betterReflection->astLocator(), $betterReflection->phpParser()),
                new PhpInternalSourceLocator(
                    $betterReflection->astLocator(),
                    new PhpStormStubsSourceStubber($betterReflection->phpParser(), 80500),
                ),
            ])),
            $phpExtensions,
        ))->extract($roots) as $class) {
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
