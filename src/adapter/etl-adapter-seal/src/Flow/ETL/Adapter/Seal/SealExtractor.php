<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Seal;

use Closure;
use CmsIg\Seal\EngineInterface;
use CmsIg\Seal\Search\Condition\Condition;
use CmsIg\Seal\Search\SearchBuilder;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Extractor;
use Flow\ETL\Extractor\Signal;
use Flow\ETL\FlowContext;
use Flow\ETL\Rows;
use Generator;

use function Flow\ETL\DSL\array_to_rows;
use function is_bool;
use function is_float;
use function is_int;
use function is_string;

final class SealExtractor implements Extractor
{
    private Direction $cursorDirection = Direction::ASC;

    private ?string $cursorField = null;

    private int $pageSize = 1000;

    /**
     * @var null|Closure(SearchBuilder): void
     */
    private ?Closure $searchBuilder = null;

    public function __construct(
        private readonly EngineInterface $engine,
        private readonly string $index,
    ) {}

    /**
     * @return Generator<int, Rows, Signal|null, void>
     */
    public function extract(FlowContext $context): Generator
    {
        if ($this->cursorField !== null) {
            yield from $this->extractWithKeyset($context, $this->cursorField, $this->cursorDirection);

            return;
        }

        yield from $this->extractWithOffset($context);
    }

    /**
     * Cursor (keyset) pagination - avoids the Elasticsearch `index.max_result_window` limit (10000 by default)
     * by paginating with a "cursor field > last seen value" filter and a constant offset of 0, instead of a
     * growing offset.
     *
     * The cursor field must hold unique values and be searchable, filterable and sortable. On Elasticsearch the
     * document identifier is mapped to `_id`, which is not indexed for range queries and therefore cannot be used
     * as a cursor - use a dedicated searchable field instead (e.g. a unique TextField).
     */
    public function withKeysetPagination(string $field, Direction $direction = Direction::ASC): self
    {
        $this->cursorField = $field;
        $this->cursorDirection = $direction;

        return $this;
    }

    public function withPageSize(int $pageSize): self
    {
        $this->pageSize = $pageSize;

        return $this;
    }

    /**
     * @param Closure(SearchBuilder): void $configure
     */
    public function withSearchBuilder(Closure $configure): self
    {
        $this->searchBuilder = $configure;

        return $this;
    }

    /**
     * @param array<string, mixed> $document
     */
    private function cursorValue(array $document, string $cursorField): string|int|float|bool
    {
        // @mago-ignore analysis:mixed-assignment
        $value = $document[$cursorField] ?? null;

        if (!is_string($value) && !is_int($value) && !is_float($value) && !is_bool($value)) {
            throw new RuntimeException(
                'Keyset cursor field "' . $cursorField . '" must hold a non-null scalar value to paginate over.',
            );
        }

        return $value;
    }

    /**
     * @return Generator<int, Rows, Signal|null, void>
     */
    private function extractWithKeyset(FlowContext $context, string $cursorField, Direction $direction): Generator
    {
        $descending = $direction === Direction::DESC;
        $cursor = null;

        while (true) {
            $search = $this->engine->createSearchBuilder($this->index);

            if ($this->searchBuilder !== null) {
                ($this->searchBuilder)($search);
            }

            $search->addSortBy($cursorField, $direction->value)->limit($this->pageSize)->offset(0);

            if ($cursor !== null) {
                $search->addFilter(
                    $descending
                        ? Condition::lessThan($cursorField, $cursor)
                        : Condition::greaterThan($cursorField, $cursor),
                );
            }

            $documents = [];
            $count = 0;

            foreach ($search->getResult() as $document) {
                $documents[] = $document;
                $cursor = $this->cursorValue($document, $cursorField);
                $count++;
            }

            if ($count === 0) {
                return;
            }

            $signal = yield array_to_rows($documents, $context->entryFactory());

            if ($signal === Signal::STOP) {
                return;
            }

            if ($count < $this->pageSize) {
                return;
            }
        }
    }

    /**
     * @return Generator<int, Rows, Signal|null, void>
     */
    private function extractWithOffset(FlowContext $context): Generator
    {
        $offset = 0;

        $search = $this->engine->createSearchBuilder($this->index);

        if ($this->searchBuilder !== null) {
            ($this->searchBuilder)($search);
        }

        $search->limit($this->pageSize);

        while (true) {
            $search->offset($offset);

            $documents = [];
            $count = 0;

            foreach ($search->getResult() as $document) {
                $documents[] = $document;
                $count++;
            }

            if ($count === 0) {
                return;
            }

            $signal = yield array_to_rows($documents, $context->entryFactory());

            if ($signal === Signal::STOP) {
                return;
            }

            if ($count < $this->pageSize) {
                return;
            }

            $offset += $this->pageSize;
        }
    }
}
