<?php

declare(strict_types=1);

namespace Flow\ETL\Window;

enum FrameBoundType
{
    case CURRENT_ROW;
    case FOLLOWING;
    case PRECEDING;
    case UNBOUNDED_FOLLOWING;
    case UNBOUNDED_PRECEDING;
}
