<?php

declare(strict_types=1);

namespace Flow\ETL\Join\HashJoin;

use Flow\ETL\Exception\DuplicatedEntriesException;
use Flow\ETL\Row;
use Flow\ETL\Row\Entries;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\Entry\Instantiators;
use Flow\ETL\Schema\Definition;

use function array_key_exists;
use function array_keys;
use function implode;
use function sprintf;

final class RowMerger
{
    /**
     * @var array<string, true>
     */
    private readonly array $dropLeft;

    /**
     * @var array<string, true>
     */
    private readonly array $dropRight;

    private readonly Instantiators $instantiators;

    /**
     * @var array<string, array{array<string, true>, array<string, true>, array<string, string>}>
     */
    private array $plans = [];

    /**
     * @var array<string, array{Definition<mixed>, Definition<mixed>}>
     */
    private array $renamedDefinitions = [];

    /**
     * @param array<string> $dropLeft - left side entries skipped in the output (duplicated join columns)
     * @param array<string> $dropRight - right side entries skipped in the output (duplicated join columns)
     */
    public function __construct(
        private readonly string $prefix = '',
        array $dropLeft = [],
        array $dropRight = [],
    ) {
        $dropLeftSet = [];

        foreach ($dropLeft as $name) {
            $dropLeftSet[$name] = true;
        }

        $dropRightSet = [];

        foreach ($dropRight as $name) {
            $dropRightSet[$name] = true;
        }

        $this->dropLeft = $dropLeftSet;
        $this->dropRight = $dropRightSet;
        $this->instantiators = new Instantiators();
    }

    /**
     * @throws DuplicatedEntriesException
     */
    public function merge(Row $left, Row $right): Row
    {
        $leftEntries = $left->entries();
        $rightEntries = $right->entries();

        $planKey = implode("\x00", $leftEntries->names()) . "\x01" . implode("\x00", $rightEntries->names());

        [$keepLeft, $keepRight, $renames] =
            $this->plans[$planKey] ??= $this->plan($leftEntries->names(), $rightEntries->names());

        $entries = [];

        foreach ($leftEntries->all() as $entry) {
            $name = $entry->name();

            if (array_key_exists($name, $keepLeft)) {
                $entries[$name] = $entry;
            }
        }

        foreach ($rightEntries->all() as $entry) {
            $name = $entry->name();

            if (!array_key_exists($name, $keepRight)) {
                continue;
            }

            if (array_key_exists($name, $renames)) {
                $entries[$renames[$name]] = $this->rename($entry, $renames[$name]);
            } else {
                $entries[$name] = $entry;
            }
        }

        return new Row(Entries::recreate($entries));
    }

    /**
     * Entry::rename() runs the full entry constructor and rebuilds the definition for every call,
     * this path instantiates the renamed entry without the constructor and reuses the renamed
     * definition as long as the source rows share the same definition instance per column.
     *
     * @param Entry<mixed> $entry
     *
     * @return Entry<mixed>
     */
    private function rename(Entry $entry, string $newName): Entry
    {
        $definition = $entry->definition();
        $cached = $this->renamedDefinitions[$newName] ?? null;

        if ($cached === null || $cached[0] !== $definition) {
            $cached = [$definition, $definition->rename($newName)];
            $this->renamedDefinitions[$newName] = $cached;
        }

        return $this->instantiators->for($entry::class)->instantiate($newName, $entry->value(), $cached[1]);
    }

    /**
     * @param array<string> $leftNames
     * @param array<string> $rightNames
     *
     * @throws DuplicatedEntriesException
     *
     * @return array{array<string, true>, array<string, true>, array<string, string>}
     */
    private function plan(array $leftNames, array $rightNames): array
    {
        $keepLeft = [];
        $outputNames = [];

        foreach ($leftNames as $name) {
            if (array_key_exists($name, $this->dropLeft)) {
                continue;
            }

            $keepLeft[$name] = true;
            $outputNames[$name] = true;
        }

        $keepRight = [];
        $renames = [];
        $rightOutputNames = [];
        $collision = false;

        foreach ($rightNames as $name) {
            if (array_key_exists($name, $this->dropRight)) {
                continue;
            }

            $keepRight[$name] = true;
            $outputName = $this->prefix === '' ? $name : $this->prefix . $name;

            if ($this->prefix !== '') {
                $renames[$name] = $outputName;
            }

            if (array_key_exists($outputName, $outputNames)) {
                $collision = true;
            }

            $outputNames[$outputName] = true;
            $rightOutputNames[] = $outputName;
        }

        if ($collision) {
            throw new DuplicatedEntriesException(sprintf(
                'Merged entries names must be unique, given: [%s] + [%s]',
                implode(', ', array_keys($keepLeft)),
                implode(', ', $rightOutputNames),
            ));
        }

        return [$keepLeft, $keepRight, $renames];
    }
}
