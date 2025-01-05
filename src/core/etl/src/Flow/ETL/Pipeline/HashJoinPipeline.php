<?php

declare(strict_types=1);

namespace Flow\ETL\Pipeline;

use function Flow\ETL\DSL\{from_rows, refs, row, rows, schema};
use Flow\ETL\Exception\{DuplicatedEntriesException, JoinException};
use Flow\ETL\Hash\NativePHPHash;
use Flow\ETL\Join\{Expression, Join};
use Flow\ETL\Pipeline\HashJoin\HashTable;
use Flow\ETL\Row\Entry;
use Flow\ETL\{DataFrame, Extractor, FlowContext, Loader, Pipeline, Row, Rows, Transformer};

final class HashJoinPipeline implements Pipeline
{
    private Extractor $extractor;

    public function __construct(
        private Pipeline $left,
        private DataFrame $right,
        private Expression $expression,
        private Join $join,
    ) {

        $this->extractor = from_rows(rows());
    }

    public function add(Loader|Transformer $pipe) : Pipeline
    {
        $this->left->add($pipe);

        return $this;
    }

    public function has(string $transformerClass) : bool
    {
        return $this->left->has($transformerClass);
    }

    public function pipes() : Pipes
    {
        return $this->left->pipes();
    }

    public function process(FlowContext $context) : \Generator
    {
        $leftReferences = refs(...$this->expression->left());
        $rightReferences = refs(...$this->expression->right());

        $hashTable = new HashTable(new NativePHPHash());

        $rightSchema = schema();

        foreach ($this->right->getEach() as $rightRow) {
            $hashTable->add($rightRow, $rightReferences);
            $rightSchema = $rightSchema->merge($rightRow->schema());
        }

        /** @var array<Entry<mixed, mixed>> $leftEntries */
        $leftEntries = [];
        /** @var array<Entry<mixed, mixed>> $rightEntries */
        $rightEntries = [];

        if ($this->join === Join::left) {
            foreach ($rightSchema->definitions() as $rightEntryDefinition) {
                $rightEntries[] = $context->entryFactory()->create($rightEntryDefinition->entry()->name(), null, $rightEntryDefinition->makeNullable());
            }
        }

        $leftSchema = schema();

        /** @var Rows $leftRows */
        foreach ($this->left->process($context) as $leftRows) {
            foreach ($leftRows as $leftRow) {
                $bucket = $hashTable->bucketFor($leftRow, $leftReferences);

                if ($bucket === null) {
                    if ($this->join === Join::left) {
                        $rightEmptyRow = row(...$rightEntries);
                        yield $this->createRows($leftRow, $rightEmptyRow);
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
                    yield $this->createRows($leftRow, $rightRow);
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
                yield $this->createRows($leftEmptyRow, $unmatchedRow);
            }
        }
    }

    public function source() : Extractor
    {
        return $this->extractor;
    }

    private function createRows(Row $leftRow, Row $rightRow) : Rows
    {
        try {
            return rows($leftRow->merge($rightRow, $this->expression->prefix()));
        } catch (DuplicatedEntriesException $e) {
            throw new JoinException($e->getMessage() . ' try to use a different join prefix than: "' . $this->expression->prefix() . '"', $e->getCode(), $e);
        }
    }
}
