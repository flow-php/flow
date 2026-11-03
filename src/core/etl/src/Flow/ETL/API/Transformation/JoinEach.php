<?php

declare(strict_types=1);

namespace Flow\ETL\API\Transformation;

use Flow\ETL\API\DataFrame;
use Flow\ETL\API\Transformation;
use Flow\ETL\Functions\Join\Expression;
use Flow\ETL\IO\Extractor\ExtractorFactory;
use Flow\ETL\Values\Join\JoinType;
use NoDiscard;

final readonly class JoinEach implements Transformation
{
    public function __construct(
        private ExtractorFactory $factory,
        private Expression $on,
        private string|JoinType $type = JoinType::left,
    ) {}

    #[NoDiscard]
    public function __invoke(DataFrame $dataFrame): DataFrame
    {
        return $dataFrame->joinEach($this->factory, $this->on, $this->type);
    }
}
