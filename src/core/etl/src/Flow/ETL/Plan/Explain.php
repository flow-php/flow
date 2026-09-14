<?php

declare(strict_types=1);

namespace Flow\ETL\Plan;

use Flow\ETL\Plan\Node\Filter;
use Flow\ETL\Plan\Node\Limit;
use Flow\ETL\Plan\Node\Offset;
use Flow\ETL\Plan\Node\Read;
use Flow\ETL\Plan\Node\Rename;
use Flow\ETL\Plan\Node\Until;
use Flow\ETL\Plan\Node\WithColumn;
use Flow\ETL\Plan\Node\Write;
use ReflectionClass;
use SplObjectStorage;

use function count;
use function implode;
use function sprintf;
use function str_repeat;

final readonly class Explain
{
    public function of(LogicalPlan $plan): string
    {
        /** @var SplObjectStorage<Node, int> $numbers */
        $numbers = new SplObjectStorage();

        return implode("\n", $this->lines($plan->root, 0, $numbers));
    }

    /**
     * @param non-negative-int $depth
     * @param SplObjectStorage<Node, int> $numbers the nodes printed so far, numbered on first visit
     *
     * @return list<string>
     */
    public function lines(Node $node, int $depth, SplObjectStorage $numbers): array
    {
        $indent = str_repeat('  ', $depth);

        if ($numbers->contains($node)) {
            return [sprintf('%s#%d (shared)', $indent, $numbers[$node])];
        }

        $numbers[$node] = $number = count($numbers) + 1;
        $lines = [sprintf('%s#%d %s', $indent, $number, $this->describe($node))];

        foreach ($node->children() as $child) {
            foreach ($this->lines($child, $depth + 1, $numbers) as $line) {
                $lines[] = $line;
            }
        }

        return $lines;
    }

    public function describe(Node $node): string
    {
        $payload = $this->payload($node);
        $redefines = $node->redefines();
        $declarations = [
            $node->rowCount()->name,
            $node->transparency()->name,
            $node->materialization()->name,
        ];

        if ($redefines->unknown) {
            $declarations[] = 'redefines unknown';
        } elseif ($redefines->names !== []) {
            $declarations[] = 'redefines ' . implode(', ', $redefines->names);
        }

        return sprintf(
            '%s%s  %s',
            (new ReflectionClass($node))->getShortName(),
            $payload === '' ? '' : '(' . $payload . ')',
            implode(' · ', $declarations),
        );
    }

    public function payload(Node $node): string
    {
        return match (true) {
            $node instanceof Read => sprintf(
                '%s, scan: limit=%s files=%s',
                (new ReflectionClass($node->extractor()))->getShortName(),
                $node->scan()->limit ?? '∅',
                (new ReflectionClass($node->scan()->pathFilter))->getShortName(),
            ),
            $node instanceof Filter, $node instanceof Until => (new ReflectionClass($node->function))->getShortName(),
            $node instanceof WithColumn => sprintf(
                '%s = %s',
                $node->name(),
                (new ReflectionClass($node->function))->getShortName(),
            ),
            $node instanceof Write => (new ReflectionClass($node->loader))->getShortName(),
            $node instanceof Limit => (string) $node->limit,
            $node instanceof Offset => (string) $node->offset,
            $node instanceof Rename => $node->from . ' → ' . $node->to,
            default => '',
        };
    }
}
