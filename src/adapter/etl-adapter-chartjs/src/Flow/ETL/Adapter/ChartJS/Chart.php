<?php declare(strict_types=1);

namespace Flow\ETL\Adapter\ChartJS;

use Flow\ETL\Row\EntryReference;
use Flow\ETL\Rows;

interface Chart
{
    public function collect(Rows $rows) : void;

    public function data() : array;

    public function setDatasetOptions(EntryReference $dataset, array $options) : self;

    public function setOptions(array $options) : self;
}
