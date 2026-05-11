<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Merge;

use Flow\PostgreSql\QueryBuilder\Condition\Condition;

interface MergeWhenStep extends MergeFinalStep
{
    public function whenMatched(): MergeWhenMatched;

    public function whenMatchedAnd(Condition $condition): MergeWhenMatched;

    public function whenNotMatched(): MergeWhenNotMatched;

    public function whenNotMatchedAnd(Condition $condition): MergeWhenNotMatched;

    public function whenNotMatchedBySource(): MergeWhenMatched;

    public function whenNotMatchedBySourceAnd(Condition $condition): MergeWhenMatched;
}
