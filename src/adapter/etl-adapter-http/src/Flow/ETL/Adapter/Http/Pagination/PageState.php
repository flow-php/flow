<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Http\Pagination;

final readonly class PageState
{
    public function __construct(
        public int $pagesFetched = 0,
        public int $resultsFetched = 0,
    ) {}

    public function advance(int $resultsThisPage): self
    {
        return new self($this->pagesFetched + 1, $this->resultsFetched + $resultsThisPage);
    }
}
