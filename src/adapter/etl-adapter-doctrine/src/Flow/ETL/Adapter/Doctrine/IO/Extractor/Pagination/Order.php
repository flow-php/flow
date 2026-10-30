<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine\IO\Extractor\Pagination;

enum Order: string
{
    case ASC = 'ASC';
    case DESC = 'DESC';
}
