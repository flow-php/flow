<?php

declare(strict_types=1);

namespace Flow\ETL\Processor;

use function Flow\ETL\DSL\{refs, row, rows, schema};
use Flow\ETL\{DataFrame, FlowContext, Processor, Row, Rows};
use Flow\ETL\Exception\{DuplicatedEntriesException, JoinException};
use Flow\ETL\Hash\NativePHPHash;
use Flow\ETL\Join\{Expression, Join};
use Flow\ETL\Processor\HashJoin\HashTable;
use Flow\ETL\Row\Entry;

/**
 * Performs hash join between upstream data and a DataFrame.
 *
 * @internal
 */
final readonly class HashJoinProcessor implements Processor
{
    public function __construct(
        private DataFrame $right,
        private Expression $expression,
        private Join $join,
    ) {
    }

    public function process(\Generator $rows, FlowContext $context) : \Generator
    {
        $leftReferences = refs(...$this->expression->left());
        $rightReferences = refs(...$this->expression->right());

        $hashTable = new HashTable(new NativePHPHash());

        $rightSchema = schema();

        foreach ($this->right->getEach() as $rightRow) {
            $hashTable->add($rightRow, $rightReferences);
            $rightSchema = $rightSchema->merge($rightRow->schema());
        }

        /** @var array<Entry<mixed>> $leftEntries */
        $leftEntries = [];
        /** @var array<Entry<mixed>> $rightEntries */
        $rightEntries = [];

        if ($this->join === Join::left) {
            foreach ($rightSchema->definitions() as $rightEntryDefinition) {
                $rightEntries[] = $context->entryFactory()->create($rightEntryDefinition->entry()->name(), null, $rightEntryDefinition->makeNullable());
            }
        }

        $leftSchema = schema();

        foreach ($rows as $leftRows) {
            /** @var Rows $leftRows */
            foreach ($leftRows as $leftRow) {
                $bucket = $hashTable->bucketFor($leftRow, $leftReferences);

                if ($bucket === null) {
                    if ($this->join === Join::left) {
                        $rightEmptyRow = row(...$rightEntries);

                        yield $this->createRows($leftRow, $rightEmptyRow, $context);
                    }

                    if ($this->join === Join::left_anti) {
                        yield rows($leftRow);
                    }

                    continue;
                }

                $rightRow = $bucket->findMatch($leftRow, $this->expression);

                if ($this->join === Join::left_anti) {
                    continue;
                }

                if ($rightRow !== null) {
                    yield $this->createRows($leftRow, $rightRow, $context);
                }
            }

            $leftSchema = $leftSchema->merge($leftRows->schema());
        }

        if ($this->join === Join::right) {
            foreach ($leftSchema->definitions() as $leftEntryDefinition) {
                $leftEntries[] = $context->entryFactory()->create($leftEntryDefinition->entry()->name(), null, $leftEntryDefinition->makeNullable());
            }

            foreach ($hashTable->unmatchedRows() as $unmatchedRow) {
                $leftEmptyRow = row(...$leftEntries);
                yield $this->createRows($leftEmptyRow, $unmatchedRow, $context);
            }
        }
    }

    private function createRows(Row $leftRow, Row $rightRow, FlowContext $context) : Rows
    {
        try {
            return match ($this->join) {
                Join::inner => rows($leftRow->merge($rightRow, $this->expression->prefix())),
                Join::left => rows($leftRow->merge($this->expression->dropDuplicateRightEntries($rightRow), $this->expression->prefix())),
                Join::right => rows($this->expression->dropDuplicateLeftEntries($leftRow)->merge($rightRow, $this->expression->prefix())),
                Join::left_anti => rows(),
            };
        } catch (DuplicatedEntriesException $e) {
            throw new JoinException($e->getMessage() . ' try to use a different join prefix than: "' . $this->expression->prefix() . '"', $e->getCode(), $e);
        }
    }
}
