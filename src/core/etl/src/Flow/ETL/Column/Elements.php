<?php

declare(strict_types=1);

namespace Flow\ETL\Column;

use function array_key_exists;

final readonly class Elements
{
    /**
     * @param list<?array<array-key, mixed>> $containers
     *
     * @return list<mixed> every element of every non-null container, in order
     */
    public function flatten(array $containers): array
    {
        $flat = [];

        foreach ($containers as $container) {
            if ($container === null) {
                continue;
            }

            // @mago-ignore analysis:mixed-assignment
            foreach ($container as $element) {
                $flat[] = $element;
            }
        }

        return $flat;
    }

    /**
     * @param list<mixed> $flat
     * @param list<?array<array-key, mixed>> $containers
     *
     * @return list<?array<array-key, mixed>> $flat cut back into the containers' shapes, keys kept, null stays null
     */
    public function shape(array $flat, array $containers): array
    {
        $shaped = [];
        $position = 0;

        foreach ($containers as $container) {
            if ($container === null) {
                $shaped[] = null;

                continue;
            }

            foreach ($container as $key => $_) {
                $container[$key] = $flat[$position++];
            }

            $shaped[] = $container;
        }

        return $shaped;
    }

    /**
     * @param list<?array<array-key, mixed>> $structures
     *
     * @return list<mixed> the element of every structure that has it
     */
    public function field(array $structures, int|string $name): array
    {
        $values = [];

        foreach ($structures as $structure) {
            if ($structure !== null && array_key_exists($name, $structure)) {
                $values[] = $structure[$name];
            }
        }

        return $values;
    }

    /**
     * @param list<?array<array-key, mixed>> $structures
     * @param list<mixed> $values field() rendered
     *
     * @return list<?array<array-key, mixed>>
     */
    public function withField(array $structures, int|string $name, array $values): array
    {
        $position = 0;

        foreach ($structures as $i => $structure) {
            if ($structure !== null && array_key_exists($name, $structure)) {
                $structures[$i][$name] = $values[$position++];
            }
        }

        return $structures;
    }
}
