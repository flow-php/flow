<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\GoogleSheet;

enum ValueInputOption : string
{
    case RAW = 'RAW';
    case USER_ENTERED = 'USER_ENTERED';

    public function toArray() : array
    {
        return ['valueInputOption' => $this->value];
    }
}
