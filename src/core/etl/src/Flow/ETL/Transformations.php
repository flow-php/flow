<?php

declare(strict_types=1);

namespace Flow\ETL;

/**
 * Collection of transformations.
 * Transformations are applied in the order they are passed to the constructor.
 */
final readonly class Transformations implements Transformation
{
    /**
     * @param Transformation ...$transformations
     */
    public array $transformations;

    public function __construct(Transformation ...$transformations)
    {
        $this->transformations = $transformations;
    }

    public function transform(DataFrame $dataFrame) : DataFrame
    {
        foreach ($this->transformations as $transformation) {
            $dataFrame = $transformation->transform($dataFrame);
        }

        return $dataFrame;
    }
}
