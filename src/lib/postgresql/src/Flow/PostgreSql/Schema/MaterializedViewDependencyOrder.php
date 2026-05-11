<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Schema;

use Flow\PostgreSql\Extractors\Tables;
use Flow\PostgreSql\Parser;
use Flow\PostgreSql\Schema\Exception\SchemaException;

/**
 * @implements ExecutionOrderStrategy<MaterializedView>
 */
final readonly class MaterializedViewDependencyOrder implements ExecutionOrderStrategy
{
    public function __construct(
        private Parser $parser,
    ) {}

    /**
     * @param list<MaterializedView> $items
     *
     * @throws SchemaException when circular materialized view dependencies are detected
     *
     * @return list<MaterializedView>
     */
    public function order(array $items): array
    {
        if (\count($items) <= 1) {
            return $items;
        }

        $viewsByName = [];

        foreach ($items as $view) {
            $viewsByName[$view->name] = $view;
        }

        /** @var array<string, list<string>> $dependsOn */
        $dependsOn = [];
        /** @var array<string, int> $inDegree */
        $inDegree = [];

        foreach ($viewsByName as $name => $view) {
            $inDegree[$name] ??= 0;
            $dependsOn[$name] = [];

            foreach ($this->extractReferencedNames($view->definition) as $refName) {
                if ($refName === $name) {
                    continue;
                }

                if (!array_key_exists($refName, $viewsByName)) {
                    continue;
                }

                $dependsOn[$name][] = $refName;
                $inDegree[$refName] ??= 0;
                $inDegree[$name]++;
            }
        }

        $queue = [];

        foreach ($inDegree as $name => $degree) {
            if ($degree === 0) {
                $queue[] = $name;
            }
        }

        $sorted = [];

        while ($queue !== []) {
            $current = \array_shift($queue);
            $sorted[] = $viewsByName[$current];

            foreach ($viewsByName as $name => $view) {
                if (\in_array($current, $dependsOn[$name], true)) {
                    $dependsOn[$name] = \array_values(\array_filter(
                        $dependsOn[$name],
                        static fn(string $dep): bool => $dep !== $current,
                    ));
                    $inDegree[$name]--;

                    if ($inDegree[$name] === 0) {
                        $queue[] = $name;
                    }
                }
            }
        }

        if (\count($sorted) !== \count($viewsByName)) {
            $unsorted = \array_diff(
                \array_keys($viewsByName),
                \array_map(static fn(MaterializedView $v): string => $v->name, $sorted),
            );

            throw new SchemaException(\sprintf('Circular dependency detected between materialized views: %s.', \implode(
                ', ',
                $unsorted,
            )));
        }

        return $sorted;
    }

    /**
     * @return list<string>
     */
    private function extractReferencedNames(string $definition): array
    {
        $tables = (new Tables($this->parser->parse($definition)))->all();
        $names = [];

        foreach ($tables as $table) {
            $names[] = $table->name();
        }

        return $names;
    }
}
