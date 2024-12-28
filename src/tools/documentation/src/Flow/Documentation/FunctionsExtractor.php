<?php

declare(strict_types=1);

namespace Flow\Documentation;

use Flow\Documentation\Models\FunctionModel;
use PhpParser\{NodeTraverser, ParserFactory, PhpVersion};

final class FunctionsExtractor
{
    public function __construct(
        private readonly string $repositoryRootPath,
        private readonly FunctionCollector $functionCollector,
    ) {
    }

    /**
     * @return \Generator<FunctionModel>
     */
    public function extract(array $paths) : \Generator
    {
        $parser = (new ParserFactory())->createForVersion(PhpVersion::fromComponents(8, 2));

        $includedFiles = get_included_files();

        foreach ($paths as $path) {
            if (false === \file_exists($path)) {
                throw new \RuntimeException(\sprintf('Path "%s" doesn\'t exists.', $path));
            }

            $realpath = \realpath($path);

            if (!\in_array($realpath, $includedFiles, true)) {
                require_once $realpath;
            }

            $ast = $parser->parse(file_get_contents($path));
            $traverser = new NodeTraverser();
            $traverser->addVisitor($this->functionCollector);
            $traverser->traverse($ast);
        }

        foreach ($this->functionCollector->functions as $functionName) {
            $reflectionFunction = new \ReflectionFunction($functionName);
            $repositoryPath = \ltrim(\str_replace($this->repositoryRootPath, '', $reflectionFunction->getFileName()), '/');

            yield FunctionModel::fromReflection(
                $repositoryPath,
                $reflectionFunction
            );
        }
    }
}
