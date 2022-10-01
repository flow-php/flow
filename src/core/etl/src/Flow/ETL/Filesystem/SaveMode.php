<?php declare(strict_types=1);

namespace Flow\ETL\Filesystem;

enum SaveMode : string
{
    /**
     * If data sink already exists, data will be appended, this solution might cause data duplication
     * since it's not check if given rows already existed.
     */
    case Append = 'append';
    /**
     * If data sink already exists error will be thrown.
     */
    case ExceptionIfExists = 'exception_if_exists';

    /**
     * If data sink already exists, writing will be skipped.
     */
    case Ignore = 'ignore';

    /**
     * If data sink already exists, it will be removed and writen again.
     */
    case Overwrite = 'overwrite';
}
