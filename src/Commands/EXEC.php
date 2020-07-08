<?php
declare(strict_types=1);

namespace Rabbit\DB\Redis\Commands;

use Predis\Command\TransactionExec;

/**
 * Class EXEC
 * @package Rabbit\DB\Redis\Commands
 */
class EXEC extends TransactionExec
{
    use ClusterSlot;
}