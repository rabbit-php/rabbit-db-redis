<?php

declare(strict_types=1);

namespace rabbit\db\redis\Commands;

use Predis\Command\ServerEval;

/**
 * Class ClusterEVAL
 * @package rabbit\db\redis\Commands
 */
class ClusterEVAL extends ServerEval
{
    use ClusterSlot;
}