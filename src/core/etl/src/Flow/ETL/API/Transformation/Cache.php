<?php

declare(strict_types=1);

namespace Flow\ETL\API\Transformation;

use Flow\ETL\API\DataFrame;
use Flow\ETL\API\Transformation;
use Flow\ETL\Storage\RowsStorage;
use NoDiscard;

final readonly class Cache implements Transformation
{
    /**
     * @param null|RowsStorage $cache reads of this cache must pass the same instance to from_cache()
     */
    public function __construct(
        private ?string $id = null,
        private ?int $cacheBatchSize = null,
        private ?RowsStorage $cache = null,
    ) {}

    #[NoDiscard]
    public function __invoke(DataFrame $dataFrame): DataFrame
    {
        return $dataFrame->cache($this->id, $this->cacheBatchSize, $this->cache);
    }
}
