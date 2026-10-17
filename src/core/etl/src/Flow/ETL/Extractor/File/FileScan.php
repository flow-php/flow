<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor\File;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Function\ScalarFunction;
use Flow\Filesystem\Path\Filter;
use Flow\Filesystem\Path\Filter\OnlyFiles;

final readonly class FileScan
{
    /**
     * @var null|int<1, max>
     */
    public ?int $limit;

    /**
     * @param null|int $limit the rows the plan needs, see Extractor::extract()
     * @param Filter $pathFilter what this read lists
     * @param null|ScalarFunction $filter a predicate over the source's columns; a source may skip what it proves
     *                                    can never match it
     *
     * @throws InvalidArgumentException when $limit is not greater than 0, or is set beside $filter: a source that
     *                                  ignores the filter would read $limit raw rows and under-deliver
     */
    public function __construct(
        ?int $limit = null,
        public Filter $pathFilter = new OnlyFiles(),
        public ?ScalarFunction $filter = null,
    ) {
        if ($limit !== null && $limit <= 0) {
            throw new InvalidArgumentException('Limit must be greater than 0');
        }

        if ($limit !== null && $filter !== null) {
            throw new InvalidArgumentException('A file scan cannot carry a limit beside a filter');
        }

        $this->limit = $limit;
    }
}
