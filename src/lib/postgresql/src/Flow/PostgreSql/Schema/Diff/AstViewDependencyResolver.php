<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Schema\Diff;

use Flow\PostgreSql\Extractors\Tables;
use Flow\PostgreSql\Parser;
use Flow\PostgreSql\Schema\Catalog;

final readonly class AstViewDependencyResolver implements ViewDependencyResolver
{
    public function __construct(private Parser $parser)
    {
    }

    public function resolve(Catalog $catalog, array $modifiedTableQualifiedNames) : DependentViews
    {
        if ($modifiedTableQualifiedNames === []) {
            return DependentViews::empty();
        }

        /** @var array<string, DependentView> $allViews */
        $allViews = [];
        /** @var array<string, list<string>> $viewDependsOn */
        $viewDependsOn = [];

        foreach ($catalog->all() as $schema) {
            foreach ($schema->views as $view) {
                $qualifiedName = $schema->name . '.' . $view->name;
                $allViews[$qualifiedName] = new DependentView($schema->name, $view);
                $viewDependsOn[$qualifiedName] = $this->extractDependencies($view->definition, $schema->name);
            }

            foreach ($schema->materializedViews as $materializedView) {
                $qualifiedName = $schema->name . '.' . $materializedView->name;
                $allViews[$qualifiedName] = new DependentView($schema->name, $materializedView);
                $viewDependsOn[$qualifiedName] = $this->extractDependencies($materializedView->definition, $schema->name);
            }
        }

        $affected = $this->findAffectedViews($modifiedTableQualifiedNames, $viewDependsOn);

        if ($affected === []) {
            return DependentViews::empty();
        }

        $depths = $this->computeDepths($affected, $viewDependsOn, $allViews);

        $dropOrder = $affected;
        \usort($dropOrder, static fn (string $a, string $b) : int => $depths[$b] <=> $depths[$a]);

        $createOrder = $affected;
        \usort($createOrder, static fn (string $a, string $b) : int => $depths[$a] <=> $depths[$b]);

        return new DependentViews(
            \array_map(static fn (string $name) : DependentView => $allViews[$name], $dropOrder),
            \array_map(static fn (string $name) : DependentView => $allViews[$name], $createOrder),
        );
    }

    /**
     * @param list<string> $affected
     * @param array<string, list<string>> $viewDependsOn
     * @param array<string, DependentView> $allViews
     *
     * @return array<string, int>
     */
    private function computeDepths(array $affected, array $viewDependsOn, array $allViews) : array
    {
        $depths = [];
        $affectedSet = \array_flip($affected);

        $getDepth = static function (string $viewName) use (&$getDepth, &$depths, $viewDependsOn, $allViews, $affectedSet) : int {
            if (\array_key_exists($viewName, $depths)) {
                return $depths[$viewName];
            }

            $depths[$viewName] = 0;
            $maxDep = -1;

            foreach ($viewDependsOn[$viewName] ?? [] as $dep) {
                if (\array_key_exists($dep, $allViews) && \array_key_exists($dep, $affectedSet)) {
                    $maxDep = \max($maxDep, $getDepth($dep));
                }
            }

            $depths[$viewName] = $maxDep + 1;

            return $depths[$viewName];
        };

        foreach ($affected as $viewName) {
            $getDepth($viewName);
        }

        return $depths;
    }

    /**
     * @return list<string>
     */
    private function extractDependencies(string $definition, string $defaultSchema) : array
    {
        $parsedQuery = $this->parser->parse($definition);
        $tables = (new Tables($parsedQuery))->all();
        $deps = [];

        foreach ($tables as $table) {
            $deps[] = ($table->schema() ?? $defaultSchema) . '.' . $table->name();
        }

        return $deps;
    }

    /**
     * @param list<string> $modifiedTableQualifiedNames
     * @param array<string, list<string>> $viewDependsOn
     *
     * @return list<string>
     */
    private function findAffectedViews(array $modifiedTableQualifiedNames, array $viewDependsOn) : array
    {
        $affected = [];
        $queue = $modifiedTableQualifiedNames;

        while ($queue !== []) {
            $current = \array_shift($queue);

            foreach ($viewDependsOn as $viewName => $deps) {
                if (\in_array($viewName, $affected, true)) {
                    continue;
                }

                if (\in_array($current, $deps, true)) {
                    $affected[] = $viewName;
                    $queue[] = $viewName;
                }
            }
        }

        return $affected;
    }
}
