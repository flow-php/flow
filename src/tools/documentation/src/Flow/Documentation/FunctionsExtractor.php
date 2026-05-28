<?php

declare(strict_types=1);

namespace Flow\Documentation;

use Flow\Documentation\Models\FunctionModel;
use Generator;
use PhpParser\NodeTraverser;
use PhpParser\ParserFactory;
use PhpParser\PhpVersion;
use ReflectionFunction;
use RuntimeException;

use function file_exists;
use function in_array;
use function ltrim;
use function realpath;
use function sprintf;
use function str_replace;

final readonly class FunctionsExtractor
{
    public function __construct(
        private string $repositoryRootPath,
        private FunctionCollector $functionCollector,
    ) {}

    /**
     * @param array<string> $paths
     *
     * @return \Generator<FunctionModel>
     */
    public function extract(array $paths): Generator
    {
        $parser = (new ParserFactory())->createForVersion(PhpVersion::fromComponents(8, 2));

        $includedFiles = get_included_files();

        foreach ($paths as $path) {
            if (false === file_exists($path)) {
                throw new RuntimeException(sprintf('Path "%s" doesn\'t exists.', $path));
            }

            $realpath = realpath($path);

            if ($realpath !== false && !in_array($realpath, $includedFiles, true)) {
                require_once $realpath;
            }

            $ast = $parser->parse((string) file_get_contents($path));

            if ($ast === null) {
                continue;
            }

            $traverser = new NodeTraverser();
            $traverser->addVisitor($this->functionCollector);
            $traverser->traverse($ast);
        }

        foreach ($this->functionCollector->functions as $functionName) {
            $reflectionFunction = new ReflectionFunction($functionName);
            $repositoryPath = ltrim(
                str_replace($this->repositoryRootPath, '', (string) $reflectionFunction->getFileName()),
                '/',
            );

            yield FunctionModel::fromReflection($repositoryPath, $reflectionFunction);
        }
    }
}
