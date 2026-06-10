<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Seal;

use Closure;
use CmsIg\Seal\EngineInterface;
use CmsIg\Seal\Search\SearchBuilder;
use Flow\ETL\Extractor;
use Flow\ETL\Extractor\Signal;
use Flow\ETL\FlowContext;
use Flow\ETL\Rows;
use Generator;

use function count;
use function Flow\ETL\DSL\array_to_rows;

final class SealExtractor implements Extractor
{
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
        $offset = 0;

        $search = $this->engine->createSearchBuilder($this->index);

        if ($this->searchBuilder !== null) {
            ($this->searchBuilder)($search);
        }

        $search->limit($this->pageSize);

        while (true) {
            $search->offset($offset);

            $documents = [];

            foreach ($search->getResult() as $document) {
                $documents[] = $document;
            }

            if ($documents === []) {
                return;
            }

            $signal = yield array_to_rows($documents, $context->entryFactory());

            if ($signal === Signal::STOP) {
                return;
            }

            if (count($documents) < $this->pageSize) {
                return;
            }

            $offset += $this->pageSize;
        }
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
}
