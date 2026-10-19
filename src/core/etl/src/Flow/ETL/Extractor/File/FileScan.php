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
     * @param null|ScalarFunction $filter a predicate over the source's columns; the source returns only rows
     *                                    matching it, and $limit counts those rows
     *
     * @throws InvalidArgumentException when $limit is not greater than 0
     */
    public function __construct(
        ?int $limit = null,
        public Filter $pathFilter = new OnlyFiles(),
        public ?ScalarFunction $filter = null,
    ) {
        if ($limit !== null && $limit <= 0) {
            throw new InvalidArgumentException('Limit must be greater than 0');
        }

        $this->limit = $limit;
    }
}
