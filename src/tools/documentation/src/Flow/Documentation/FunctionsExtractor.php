<?php

declare(strict_types=1);

namespace Flow\Documentation;

use Flow\Documentation\Models\FunctionModel;
use Generator;
use Roave\BetterReflection\Identifier\IdentifierType;
use Roave\BetterReflection\Reflection\ReflectionFunction;
use Roave\BetterReflection\Reflector\Reflector;
use Roave\BetterReflection\SourceLocator\Ast\Locator;
use Roave\BetterReflection\SourceLocator\Type\SingleFileSourceLocator;
use RuntimeException;

use function file_exists;
use function Flow\Types\DSL\type_instance_of;
use function ltrim;
use function realpath;
use function sprintf;
use function str_replace;

final readonly class FunctionsExtractor
{
    public function __construct(
        private string $repositoryRootPath,
        private Reflector $reflector,
        private Locator $astLocator,
    ) {}

    /**
     * @param array<non-empty-string> $paths
     *
     * @return \Generator<FunctionModel>
     */
    public function extract(array $paths): Generator
    {
        foreach ($paths as $path) {
            if (false === file_exists($path)) {
                throw new RuntimeException(sprintf('Path "%s" doesn\'t exists.', $path));
            }

            $repositoryPath = ltrim(str_replace($this->repositoryRootPath, '', (string) realpath($path)), '/');

            foreach ((new SingleFileSourceLocator($path, $this->astLocator))->locateIdentifiersByType(
                $this->reflector,
                new IdentifierType(IdentifierType::IDENTIFIER_FUNCTION),
            ) as $reflectionFunction) {
                yield FunctionModel::fromReflection(
                    $repositoryPath,
                    type_instance_of(ReflectionFunction::class)->assert($reflectionFunction),
                );
            }
        }
    }
}
