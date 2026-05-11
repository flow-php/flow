<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Schema;

use Flow\PostgreSql\Schema\Exception\SchemaException;

/**
 * @implements ExecutionOrderStrategy<Table>
 */
final readonly class ForeignKeyDependencyOrder implements ExecutionOrderStrategy
{
    /**
     * Orders tables so that referenced tables come before tables that reference them.
     * Uses Kahn's algorithm for topological sorting.
     *
     * @param list<Table> $items
     *
     * @throws SchemaException when circular foreign key dependencies are detected
     *
     * @return list<Table>
     */
    public function order(array $items): array
    {
        if (\count($items) <= 1) {
            return $items;
        }

        $tablesByQualifiedName = [];

        foreach ($items as $table) {
            $tablesByQualifiedName[$table->qualifiedName()] = $table;
        }

        /** @var array<string, list<string>> $dependsOn qualified name → list of qualified names it depends on */
        $dependsOn = [];
        /** @var array<string, int> $inDegree */
        $inDegree = [];

        foreach ($tablesByQualifiedName as $qualifiedName => $table) {
            $inDegree[$qualifiedName] ??= 0;
            $dependsOn[$qualifiedName] = [];

            foreach ($table->foreignKeys as $fk) {
                $refQualified = $fk->referenceSchema . '.' . $fk->referenceTable;

                if ($refQualified === $qualifiedName) {
                    continue;
                }

                if (!array_key_exists($refQualified, $tablesByQualifiedName)) {
                    continue;
                }

                $dependsOn[$qualifiedName][] = $refQualified;
                $inDegree[$refQualified] ??= 0;
                $inDegree[$qualifiedName]++;
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
            $sorted[] = $tablesByQualifiedName[$current];

            foreach ($tablesByQualifiedName as $name => $table) {
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

        if (\count($sorted) !== \count($tablesByQualifiedName)) {
            $unsorted = \array_diff(
                \array_keys($tablesByQualifiedName),
                \array_map(static fn(Table $t): string => $t->qualifiedName(), $sorted),
            );

            throw new SchemaException(\sprintf('Circular foreign key dependency detected between tables: %s. Use deferred constraints to handle circular references.', \implode(
                ', ',
                $unsorted,
            )));
        }

        return $sorted;
    }
}
