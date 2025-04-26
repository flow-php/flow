<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine\Pagination;

enum Order : string
{
    case ASC = 'ASC';
    case DESC = 'DESC';
}
